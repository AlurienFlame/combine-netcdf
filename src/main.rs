use netcdf_sys::{
    NC_memio, nc_close_memio, nc_create_mem, nc_def_dim, nc_def_var_endian, nc_enddef, nc_get_var, nc_inq, nc_inq_ndims, nc_inq_type, nc_inq_var, nc_inq_var_endian, nc_strerror
};
use rocket::State;
use rocket::response::status;
use std::{collections::HashMap, sync::Mutex};

#[macro_use]
extern crate rocket;

struct Parts {
    part_a: Option<Vec<u8>>,
    part_b: Option<Vec<u8>>,
}
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
    unsafe { nc_def_dim(base, name, len, &mut idp) };
    println!(
        "Defined dimension '{}' of length {} with id {}",
        unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy() },
        len,
        idp
    );
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
    unsafe { nc_inq_ndims(source, &mut var_num_dims) };
    let mut dimids: Vec<libc::c_int> = vec![0; var_num_dims as usize];
    let mut var_num_attrs: libc::c_int = 0;
    unsafe {
        nc_inq_var(
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
    unsafe {
        netcdf_sys::nc_def_var(
            base,
            var_name.as_mut_ptr(),
            var_type,
            var_num_dims,
            dimids.as_mut_ptr(),
            &mut new_varid,
        )
    };
    
    // Match endian-ness
    // let mut endianp: libc::c_int = 0;
    // unsafe { nc_inq_var_endian(file, varid, &mut endianp) };
    // unsafe { nc_def_var_endian(output, new_varid, endianp) };
    
    // Copy variable attributes
    for attr_idx in 0..var_num_attrs {
        merge_attrs(source, base, attr_idx, varid, new_varid);
    }
}

// Merges two netCDF files (represented by their ncid) by copying the the second one onto the first
fn merge_onto(output: libc::c_int, file: libc::c_int) -> () {
    // Get info about the file we're copying from
    let mut num_dims: libc::c_int = 0;
    let mut num_vars: libc::c_int = 0;
    let mut num_global_atts: libc::c_int = 0;
    let mut unlimdimidp: libc::c_int = 0;
    unsafe {
        nc_inq(
            file,
            &mut num_dims,
            &mut num_vars,
            &mut num_global_atts,
            &mut unlimdimidp,
        );
    }

    // Copy global attributes
    for attr_idx in 0..num_global_atts {
        merge_attrs(
            file,
            output,
            attr_idx,
            netcdf_sys::NC_GLOBAL,
            netcdf_sys::NC_GLOBAL,
        );
    }

    // Copy dimensions
    let mut dimids = 0; // this should be an array but we don't use it anyway
    unsafe { netcdf_sys::nc_inq_dimids(file, &mut num_dims, &mut dimids, 0) };
    for dimid in 0..num_dims {
        merge_dims(file, output, dimid);
    }

    // Copy variables
    for varid in 0..num_vars {
        merge_var_definitions(file, output, varid);
    }
    
    // End define mode
    // This must be done before we can write variable data
    unsafe { nc_enddef(output) };
    
    for varid in 0..num_vars {
        // Get variable data
        let mut var_name: Vec<libc::c_char> = vec![0; 256];
        let mut var_type: netcdf_sys::nc_type = 0;
        let mut var_num_dims: libc::c_int = 0;
        // We need to grab ndims before making our final inquiry so we can size our dimids vector
        unsafe { nc_inq_ndims(file, &mut var_num_dims) };
        let mut dimids: Vec<libc::c_int> = vec![0; var_num_dims as usize];
        let mut var_num_attrs: libc::c_int = 0;
        unsafe {
            nc_inq_var(
                file,
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
            nc_inq_type(file, var_type, var_type_name.as_mut_ptr(), &mut var_type_size);
        }
        let mut var_length = 1;
        for dim_index in 0..var_num_dims {
            // Calculate total number of elements stored in the variable, which is the product of its dimension lengths
            let dimid = dimids[dim_index as usize];
            let mut dim_len = 0;
            unsafe { netcdf_sys::nc_inq_dimlen(file, dimid, &mut dim_len) };
            var_length *= dim_len;
        }
        let mut buffer: Vec<u8> = vec![0; var_type_size * var_length];
        unsafe {
            nc_get_var(
                file,
                varid,
                buffer.as_mut_ptr() as *mut std::os::raw::c_void,
            )
        };
        // TODO: merge with existing data along an unlimited dimension
        unsafe {
            netcdf_sys::nc_put_var(
                output,
                varid, // Danger! This assumes variable IDs are the same in both files
                buffer.as_ptr() as *const std::os::raw::c_void,
            )
        };
    }
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
            std::ffi::CStr::from_ptr(nc_strerror(status_a)).to_string_lossy()
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
            std::ffi::CStr::from_ptr(nc_strerror(status_b)).to_string_lossy()
        });
    }
    println!("Opened part B with ncid {}", file_b);

    // create a new file to hold the merged data
    // I could probably just clone A and merge onto it, but this way I can be sure that I'm writing everything
    let mut output = -1;
    let status = unsafe { nc_create_mem("output.nc\0".as_ptr().cast(), 0, 0, &mut output) };
    if status != 0 {
        panic!("Failed to create output file: {}", unsafe {
            std::ffi::CStr::from_ptr(nc_strerror(status)).to_string_lossy()
        });
    }
    println!("Created output file with ncid {}", output);

    // Copy data over from both files
    merge_onto(output, file_a);
    // merge_onto(output, file_b);

    // Check information about output file
    let mut ndimsp = 0;
    let mut nvarsp = 0;
    let mut nattsp = 0;
    let mut unlimdimidp = 0;
    unsafe {
        nc_inq(
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
    let mut info: NC_memio = unsafe { std::mem::zeroed() };
    let status = unsafe { nc_close_memio(output, &mut info) };
    if status != 0 {
        panic!("Failed to close output file: {}", unsafe {
            std::ffi::CStr::from_ptr(nc_strerror(status)).to_string_lossy()
        });
    }
    println!("Closed output file {}", output);
    let accessible_info = unsafe { &*(&info as *const NC_memio as *const AccessibleMemio) };
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
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![part_a, part_b, read])
        .manage(AppState {
            storage: Mutex::new(HashMap::new()),
        })
}
