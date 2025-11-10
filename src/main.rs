use netcdf_sys::{NC_memio, nc_close_memio, nc_create_mem, nc_def_dim, nc_inq, nc_strerror};
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
        let name = name_buf.as_mut_ptr();
        unsafe { netcdf_sys::nc_inq_varname(file, varid, name) };
        let mut xtypep = 0;
        unsafe { netcdf_sys::nc_inq_vartype(file, varid, &mut xtypep) };
        let mut ndims = 0;
        unsafe { netcdf_sys::nc_inq_varndims(file, varid, &mut ndims) };
        let mut dimids: Vec<i32> = vec![0; ndims as usize];
        unsafe { netcdf_sys::nc_inq_vardimid(file, varid, dimids.as_mut_ptr()) };
        let mut new_varid = -1;
        unsafe {
            netcdf_sys::nc_def_var(
                output,
                name,
                xtypep,
                ndims,
                dimids.as_mut_ptr(),
                &mut new_varid,
            )
        };

        // Copy variable data
        // TODO
    }

    // Copy attributes
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
