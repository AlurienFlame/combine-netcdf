use netcdf_sys::{
    NC_memio, nc_close_memio, nc_create_mem, nc_def_dim, nc_inq, nc_inq_ndims, nc_inq_var,
    nc_strerror,
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

// Merges two netCDF files (represented by their ncid) by copying the the second one onto the first
fn merge_onto(output: i32, file: i32) -> () {
    // Copy dimensions
    let mut ndims = 0;
    let mut dimids = 0;
    unsafe { netcdf_sys::nc_inq_dimids(file, &mut ndims, &mut dimids, 0) };
    for dimid in 0..ndims {
        let mut name_buf: Vec<libc::c_char> = vec![0; 256];
        let name = name_buf.as_mut_ptr();
        unsafe { netcdf_sys::nc_inq_dimname(file, dimid, name) };
        let mut len = 0;
        unsafe { netcdf_sys::nc_inq_dimlen(file, dimid, &mut len) };
        let mut idp = -1;
        unsafe { nc_def_dim(output, name, len, &mut idp) };
    }

    // Copy variables
    let mut nvarsp = 0;
    unsafe { netcdf_sys::nc_inq_nvars(file, &mut nvarsp) };
    for varid in 0..nvarsp {
        let mut name_buf: Vec<libc::c_char> = vec![0; 256];
        let mut xtypep: netcdf_sys::nc_type = 0; // typeid
        let mut ndims: libc::c_int = 0; // number of dimensions
        // We need to grab ndims before making our final inquiry so we can size our dimids vector
        unsafe { nc_inq_ndims(file, &mut ndims) };
        let mut dimids: Vec<libc::c_int> = vec![0; ndims as usize]; // dimension IDs
        let mut nattsp: libc::c_int = 0; // number of attributes
        unsafe {
            nc_inq_var(
                file,
                varid,
                name_buf.as_mut_ptr(),
                &mut xtypep,
                &mut ndims,
                dimids.as_mut_ptr(),
                &mut nattsp,
            )
        };
        let mut new_varid = -1;
        unsafe {
            netcdf_sys::nc_def_var(
                output,
                name_buf.as_mut_ptr(),
                xtypep,
                ndims,
                dimids.as_mut_ptr(),
                &mut new_varid,
            )
        };

        // Copy variable attributes
        for attid in 0..nattsp {
            let mut att_name_buf: Vec<libc::c_char> = vec![0; 256];
            let att_name = att_name_buf.as_mut_ptr();
            let mut xtypep = 0;
            let mut lenp = 0; // number of values currently stored in the attribute
            unsafe {
                netcdf_sys::nc_inq_attname(file, varid, attid, att_name);
                netcdf_sys::nc_inq_att(file, varid, att_name, &mut xtypep, &mut lenp);
            }
            let mut type_name_buf: Vec<libc::c_char> = vec![0; 256];
            let type_name = type_name_buf.as_mut_ptr();
            let mut type_size = 0;
            unsafe { netcdf_sys::nc_inq_type(file, xtypep, type_name, &mut type_size) };
            let mut data: Vec<u8> = vec![0; lenp * type_size];

            unsafe {
                netcdf_sys::nc_get_att(
                    file,
                    varid,
                    att_name,
                    data.as_mut_ptr() as *mut std::os::raw::c_void,
                )
            };
            unsafe {
                netcdf_sys::nc_put_att(
                    output,
                    new_varid,
                    att_name,
                    xtypep,
                    lenp,
                    data.as_ptr() as *const std::os::raw::c_void,
                )
            };
        }

        // Copy variable data
        // TODO
    }

    // Copy global attributes
    // TODO
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
        panic!("Output memory is null"); // FIXME: triggering
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
