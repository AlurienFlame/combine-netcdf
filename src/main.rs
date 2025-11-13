use netcdf_sys::nc_copy_var;
use rocket::State;
use rocket::response::status;
use std::{collections::HashMap, sync::Mutex};

#[macro_use]
extern crate rocket;

struct Parts {
    part_a: Option<Vec<u8>>,
    part_b: Option<Vec<u8>>,
}

// Switch to something like a DashMap to scale up
struct AppState {
    storage: Mutex<HashMap<String, Parts>>,
}

#[post("/part_a?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_a(name: String, input: Vec<u8>, state: &State<AppState>) -> status::Accepted<String> {
    // Dig up the relevant part of state, or create it if necessary
    let mut storage = state.storage.lock().unwrap();
    let entry = storage.entry(name.clone()).or_insert_with(|| Parts {
        part_a: None,
        part_b: None,
    });
    // Update state with the uploaded file
    entry.part_a = Some(input);
    let byte_count = entry.part_a.as_ref().unwrap().len();
    status::Accepted(format!("received: '{}' ({} bytes)", name, byte_count))
}

#[post("/part_b?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_b(name: String, input: Vec<u8>, state: &State<AppState>) -> status::Accepted<String> {
    // Dig up the relevant part of state, or create it if necessary
    let mut storage = state.storage.lock().unwrap();
    let entry = storage.entry(name.clone()).or_insert_with(|| Parts {
        part_a: None,
        part_b: None,
    });
    // Update state with the uploaded file
    entry.part_b = Some(input);
    let byte_count = entry.part_a.as_ref().unwrap().len();
    status::Accepted(format!("received: '{}' ({} bytes)", name, byte_count))
}

fn merge_dims(source: libc::c_int, base: libc::c_int, source_dimid: libc::c_int) -> () {
    let mut name_buf: Vec<libc::c_char> = vec![0; 256];
    let name = name_buf.as_mut_ptr();
    unsafe { netcdf_sys::nc_inq_dimname(source, source_dimid, name) };
    let mut len = 0;
    unsafe { netcdf_sys::nc_inq_dimlen(source, source_dimid, &mut len) };
    let mut idp = -1;
    unsafe { netcdf_sys::nc_def_dim(base, name, len, &mut idp) };
    // Here and anywhere else where the definition will already exist in the second run-through,
    // we just ignore the error. A check could be inserted without much difficulty, but it wouldn't do much.
}

fn merge_attrs(
    source: libc::c_int,
    base: libc::c_int,
    source_idx: libc::c_int,
    source_varid: libc::c_int,
    base_varid: libc::c_int,
) -> () {
    let mut attr_name: Vec<libc::c_char> = vec![0; 256];
    let mut attr_type: netcdf_sys::nc_type = 0;
    let mut attr_len: usize = 0; // number of values currently stored in the attribute
    unsafe {
        netcdf_sys::nc_inq_attname(source, source_varid, source_idx, attr_name.as_mut_ptr());
        netcdf_sys::nc_inq_att(
            source,
            source_varid,
            attr_name.as_ptr(),
            &mut attr_type,
            &mut attr_len,
        );
    };
    let mut type_name: Vec<libc::c_char> = vec![0; 256];
    let mut type_size: usize = 0;
    unsafe { netcdf_sys::nc_inq_type(source, attr_type, type_name.as_mut_ptr(), &mut type_size) };
    let mut data: Vec<u8> = vec![0; attr_len * type_size];

    unsafe {
        netcdf_sys::nc_get_att(
            source,
            source_varid,
            attr_name.as_ptr(),
            data.as_mut_ptr() as *mut std::os::raw::c_void,
        )
    };
    unsafe {
        netcdf_sys::nc_put_att(
            base,
            base_varid,
            attr_name.as_mut_ptr(),
            attr_type,
            attr_len,
            data.as_ptr() as *const std::os::raw::c_void,
        )
    };
}

fn merge_var_definitions(source: libc::c_int, base: libc::c_int, varid: libc::c_int) {
    // Get variable data
    let mut var_name: Vec<libc::c_char> = vec![0; 256];
    let mut var_type: netcdf_sys::nc_type = 0;
    let mut var_num_dims: libc::c_int = 0;
    // We need to grab ndims before making our final inquiry so we can size our dimids vector
    unsafe { netcdf_sys::nc_inq_ndims(source, &mut var_num_dims) };
    let mut dimids: Vec<libc::c_int> = vec![0; var_num_dims as usize];
    let mut var_num_attrs: libc::c_int = 0;
    unsafe {
        netcdf_sys::nc_inq_var(
            source,
            varid,
            var_name.as_mut_ptr(),
            &mut var_type,
            &mut var_num_dims,
            dimids.as_mut_ptr(),
            &mut var_num_attrs,
        )
    };

    // Copy variable definitions
    let mut new_varid: libc::c_int = -1;
    let status = unsafe {
        netcdf_sys::nc_def_var(
            base,
            var_name.as_mut_ptr(),
            var_type,
            var_num_dims,
            dimids.as_mut_ptr(),
            &mut new_varid,
        )
    };
    if status == netcdf_sys::NC_ENAMEINUSE {
        unsafe { netcdf_sys::nc_inq_varid(base, var_name.as_ptr(), &mut new_varid) };
    } else if status != 0 {
        panic!("Failed to define variable: code {}: {}", status, unsafe {
            std::ffi::CStr::from_ptr(netcdf_sys::nc_strerror(status)).to_string_lossy()
        });
    }

    // Copy variable attributes
    for attr_idx in 0..var_num_attrs {
        merge_attrs(source, base, attr_idx, varid, new_varid);
    }

    // Match chunking settings
    let mut storagep: libc::c_int = 0;
    let mut chunksizes: Vec<libc::size_t> = vec![0; var_num_dims as usize];
    unsafe {
        netcdf_sys::nc_inq_var_chunking(source, varid, &mut storagep, chunksizes.as_mut_ptr())
    };
    unsafe { netcdf_sys::nc_def_var_chunking(base, new_varid, storagep, chunksizes.as_mut_ptr()) };

    // Match deflation settings
    let mut shufflep: libc::c_int = 0;
    let mut deflatep: libc::c_int = 0;
    let mut deflate_levelp: libc::c_int = 0;
    unsafe {
        netcdf_sys::nc_inq_var_deflate(
            source,
            varid,
            &mut shufflep,
            &mut deflatep,
            &mut deflate_levelp,
        )
    };
    unsafe {
        netcdf_sys::nc_def_var_deflate(base, new_varid, shufflep, deflatep, deflate_levelp)
    };

    // TODO: Match other compression settings (fletcher32, szip, etc.) and other stuff like chunking and endianness
}

fn merge_var_data(source: libc::c_int, base: libc::c_int, varid: libc::c_int) {
    // Get variable data
    let mut var_name: Vec<libc::c_char> = vec![0; 256];
    let mut var_type: netcdf_sys::nc_type = 0;
    let mut var_num_dims: libc::c_int = 0;
    // We need to grab ndims before making our final inquiry so we can size our dimids vector
    unsafe { netcdf_sys::nc_inq_ndims(source, &mut var_num_dims) };
    let mut dimids: Vec<libc::c_int> = vec![0; var_num_dims as usize];
    let mut var_num_attrs: libc::c_int = 0;
    unsafe {
        netcdf_sys::nc_inq_var(
            source,
            varid,
            var_name.as_mut_ptr(),
            &mut var_type,
            &mut var_num_dims,
            dimids.as_mut_ptr(),
            &mut var_num_attrs,
        )
    };

    // Copy variable data
    let mut var_type_size = 0;
    let mut var_type_name: Vec<libc::c_char> = vec![0; 256];
    unsafe {
        netcdf_sys::nc_inq_type(
            source,
            var_type,
            var_type_name.as_mut_ptr(),
            &mut var_type_size,
        );
    }
    let mut var_length = 1;
    for dim_index in 0..var_num_dims {
        // Calculate total number of elements stored in the variable, which is the product of its dimension lengths
        let dimid = dimids[dim_index as usize];
        let mut dim_len = 0;
        unsafe { netcdf_sys::nc_inq_dimlen(source, dimid, &mut dim_len) };
        var_length *= dim_len;
    }
    let mut buffer: Vec<u8> = vec![0; var_type_size * var_length];
    unsafe {
        netcdf_sys::nc_get_var(
            source,
            varid,
            buffer.as_mut_ptr() as *mut std::os::raw::c_void,
        )
    };
    unsafe {
        netcdf_sys::nc_put_var(
            base,
            varid, // Danger! This assumes variable IDs are the same in both files
            buffer.as_ptr() as *const std::os::raw::c_void,
        )
    };
}

// Merges two netCDF files (represented by their ncid) by copying the the second one onto the first
fn merge_files(base: libc::c_int, source: libc::c_int) -> () {
    // Get info about the file we're copying from
    let mut num_dims: libc::c_int = 0;
    let mut num_vars: libc::c_int = 0;
    let mut num_global_atts: libc::c_int = 0;
    let mut unlimdimidp: libc::c_int = 0;
    unsafe {
        netcdf_sys::nc_inq(
            source,
            &mut num_dims,
            &mut num_vars,
            &mut num_global_atts,
            &mut unlimdimidp,
        );
    }

    // Pop into data mode. We're in this by default, but it might not be the first time we've run this function.
    unsafe { netcdf_sys::nc_redef(base) };

    // Copy global attributes
    for attr_idx in 0..num_global_atts {
        merge_attrs(
            source,
            base,
            attr_idx,
            netcdf_sys::NC_GLOBAL,
            netcdf_sys::NC_GLOBAL,
        );
    }

    // Copy dimensions
    let mut dimids = 0; // this should be an array but we don't use it anyway
    unsafe { netcdf_sys::nc_inq_dimids(source, &mut num_dims, &mut dimids, 0) };
    for dimid in 0..num_dims {
        merge_dims(source, base, dimid);
    }

    // Copy variables

    // Now, I know what you're thinking. Why not use nc_copy_var?
    // Well, firstly, I didn't realize that existed until after I had this solution working.
    // Still, I tried it afterwards, and it turns out it's exponentially slower. I'm not sure why.
    for varid in 0..num_vars {
        merge_var_definitions(source, base, varid);
    }
    unsafe { netcdf_sys::nc_enddef(base) };

    for varid in 0..num_vars {
        merge_var_data(source, base, varid);
    }

    print!("Finished merging file ncid {} into ncid {}\n", source, base);
}

// Reimplement NC_memio to circumvent privacy issue
pub struct AccessibleMemio {
    size: usize,
    memory: *mut std::os::raw::c_void,
    _flags: std::os::raw::c_int,
}

fn merge_parts(part_a: &Vec<u8>, part_b: &Vec<u8>) -> Vec<u8> {
    // Load bytes as netCDF files
    let mut file_a = -1;
    let status_a = unsafe {
        netcdf_sys::nc_open_mem(
            "part_a.nc\0".as_ptr().cast(),
            0,
            part_a.len(),
            part_a.as_ptr() as *mut std::os::raw::c_void,
            &mut file_a,
        )
    };
    if status_a != 0 {
        panic!("Failed to open part A: {}", unsafe {
            std::ffi::CStr::from_ptr(netcdf_sys::nc_strerror(status_a)).to_string_lossy()
        });
    }
    println!("Opened part A with ncid {}", file_a);

    let mut file_b = -1;
    let status_b = unsafe {
        netcdf_sys::nc_open_mem(
            "part_b.nc\0".as_ptr().cast(),
            0,
            part_b.len(),
            part_b.as_ptr() as *mut std::os::raw::c_void,
            &mut file_b,
        )
    };

    if status_b != 0 {
        panic!("Failed to open part B: {}", unsafe {
            std::ffi::CStr::from_ptr(netcdf_sys::nc_strerror(status_b)).to_string_lossy()
        });
    }
    println!("Opened part B with ncid {}", file_b);

    // TODO: Error check for file incompatibilities

    // create a new file to hold the merged data
    // I could probably just clone A and merge onto it, but this way I can be sure that I'm writing everything
    let mut formatp: libc::c_int = 0;
    unsafe { netcdf_sys::nc_inq_format(file_a, &mut formatp) };
    let flags: libc::c_int;
    match formatp {
        netcdf_sys::NC_FORMAT_CLASSIC => {
            flags = netcdf_sys::NC_CLASSIC_MODEL;
        }
        netcdf_sys::NC_FORMAT_64BIT_OFFSET => {
            flags = netcdf_sys::NC_64BIT_OFFSET;
        }
        netcdf_sys::NC_FORMAT_CDF5 => {
            flags = netcdf_sys::NC_CDF5; // == NC_64BIT_DATA
        }
        netcdf_sys::NC_FORMAT_NETCDF4 => {
            flags = netcdf_sys::NC_NETCDF4;
        }
        netcdf_sys::NC_FORMAT_NETCDF4_CLASSIC => {
            // I have no idea if this is even allowed
            flags = netcdf_sys::NC_NETCDF4 | netcdf_sys::NC_CLASSIC_MODEL;
        }
        _ => {
            panic!("Unknown netCDF format: {}", formatp);
        }
    }

    let mut formatp_b: libc::c_int = 0;
    unsafe { netcdf_sys::nc_inq_format(file_b, &mut formatp_b) };
    if formatp != formatp_b {
        println!(
            "Input files have different formats: {} vs {}",
            formatp, formatp_b
        );
    }

    let mut output = -1;
    let status =
        unsafe { netcdf_sys::nc_create_mem("output.nc\0".as_ptr().cast(), flags, 0, &mut output) };
    if status != 0 {
        panic!("Failed to create output file: {}", unsafe {
            std::ffi::CStr::from_ptr(netcdf_sys::nc_strerror(status)).to_string_lossy()
        });
    }
    println!("Created output file with ncid {}", output);

    // Copy data over from both files
    merge_files(output, file_a);
    merge_files(output, file_b);

    // Check information about output file
    let mut ndimsp = 0;
    let mut nvarsp = 0;
    let mut nattsp = 0;
    let mut unlimdimidp = 0;
    unsafe {
        netcdf_sys::nc_inq(
            output,
            &mut ndimsp,
            &mut nvarsp,
            &mut nattsp,
            &mut unlimdimidp,
        );
    }
    println!(
        "Output file info - ndims: {}, nvars: {}, natts: {}, unlimdimid: {}",
        ndimsp, nvarsp, nattsp, unlimdimidp
    );

    // Write output to memory
    let mut info: netcdf_sys::NC_memio = unsafe { std::mem::zeroed() };
    let status = unsafe { netcdf_sys::nc_close_memio(output, &mut info) };
    if status != 0 {
        panic!("Failed to close output file: {}", unsafe {
            std::ffi::CStr::from_ptr(netcdf_sys::nc_strerror(status)).to_string_lossy()
        });
    }
    println!("Closed output file {}", output);
    let accessible_info =
        unsafe { &*(&info as *const netcdf_sys::NC_memio as *const AccessibleMemio) };
    println!("Output size: {}", accessible_info.size);

    if accessible_info.size > isize::MAX as usize {
        panic!("Output size exceeds isize::MAX");
    };

    if accessible_info.memory.is_null() {
        panic!("Output memory is null");
    };

    let output_bytes = unsafe {
        std::slice::from_raw_parts(accessible_info.memory as *const u8, accessible_info.size)
            .to_vec()
    };

    if !accessible_info.memory.is_null() {
        unsafe {
            libc::free(accessible_info.memory);
        }
    }

    output_bytes
}

#[get("/read?<name>")]
fn read(name: &str, state: &State<AppState>) -> Vec<u8> {
    let storage = state.storage.lock().unwrap();
    let part_a = storage
        .get(name)
        .and_then(|entry| entry.part_a.as_ref())
        .expect("Part A not found");
    let part_b = storage
        .get(name)
        .and_then(|entry| entry.part_b.as_ref())
        .expect("Part B not found");
    merge_parts(part_a, part_b)
    // TODO: replace panics with error responses throughout
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![part_a, part_b, read])
        .manage(AppState {
            storage: Mutex::new(HashMap::new()),
        })
}
