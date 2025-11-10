use netcdf_sys::{
    NC_memio, nc_close_memio, nc_create, nc_create_mem, nc_def_dim, nc_enddef, nc_inq_format, nc_strerror
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
    let status = unsafe {
        nc_create_mem(
            "output.nc\0".as_ptr().cast(),
            0,
            0,
            &mut output,
        )
    };
    if status != 0 {
        panic!("Failed to create output file: {}", unsafe {
            std::ffi::CStr::from_ptr(nc_strerror(status)).to_string_lossy()
        });
    }
    println!("Created output file with ncid {}", output);

    // Write some dummy data
    let mut time_dimid = -1;
    let status = unsafe { nc_def_dim(output, "time\0".as_ptr().cast(), 10, &mut time_dimid) };
    println!(
        "Defined dimension 'time' with id {} (status {})",
        time_dimid, status
    );

    let mut temp_varid = -1;
    let status = unsafe {
        netcdf_sys::nc_def_var(
            output,
            "temperature\0".as_ptr().cast(),
            netcdf_sys::NC_FLOAT,
            1,
            &time_dimid,
            &mut temp_varid,
        )
    };
    println!(
        "Defined variable 'temperature' with id {} (status {})",
        temp_varid, status
    );

    // let temp_data: Vec<f32> = (0..10).map(|i| i as f32 * 1.5).collect();
    // let status = unsafe {
    //     netcdf_sys::nc_put_var_float(
    //         output,
    //         temp_varid,
    //         temp_data.as_ptr() as *const f32,
    //     )
    // };
    // println!("Wrote data to 'temperature' variable (status {})", status);

    // Copy data over from both files
    // for file in [file_a, file_b] {
    //     // Copy dimensions
    //     let mut ndims = 0;
    //     let mut dimids: Vec<i32> = Vec::new();
    //     unsafe { netcdf_sys::nc_inq_dimids(file, &mut ndims, dimids.as_mut_ptr(), 0) };
    //     for dimid in dimids {
    //         let mut name= -1;
    //         unsafe { nc_inq_dimname(file, dimid, &mut name) };
    //         let mut len = 0;
    //         unsafe { nc_inq_dimlen(file, dimid, &mut len) };
    //         let mut idp = -1;
    //         unsafe { nc_def_dim(output, &mut name, len, &mut idp) };
    //     }

    // Copy variables
    // for var in file.variables() {
    //     let name = var.name();
    // if output.variable(&name).is_none() {
    //     let var_type = var.vartype();
    //     let dim_ids: Vec<DimensionIdentifier> = var
    //         .dimensions()
    //         .into_iter()
    //         .map(|d| d.identifier())
    //         .collect();
    //     let mut new_var = match output
    //         .add_variable_from_identifiers_with_type(&name, &dim_ids, &var_type)
    //     {
    //         Ok(v) => v,
    //         Err(e) => {
    //             eprintln!("Failed to add variable '{}': {}", name, e);
    //             // Skip this variable on error
    //             continue;
    //         }
    //     };

    //     // Copy variable data
    //     let data: Vec<u8> = var.get_raw_values(netcdf::Extents::All).unwrap();
    //     new_var.put_values(&data, netcdf::Extents::All).unwrap();
    // }
    // }

    // Copy attributes
    // for attr in file.attributes() {
    //     output
    //         .add_attribute::<AttributeValue>(&attr.name(), attr.value().unwrap())
    //         .unwrap();
    // }
    // }

    unsafe {
        nc_enddef(output);
    }

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
