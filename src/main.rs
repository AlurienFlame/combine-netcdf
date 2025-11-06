use netcdf::{AttributeValue, DimensionIdentifier};
use rocket::{State, serde};
use rocket::http::ext::IntoCollection;
use rocket::{response::status};
use std::fmt::Debug;
use std::sync::Arc;
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

fn merge_parts(part_a: &[u8], part_b: &[u8]) -> Vec<u8> {
    // Load bytes as netCDF files
    let file_a = netcdf::open_mem(Some("part_a"), part_a).expect("Failed to open part A");
    let file_b = netcdf::open_mem(Some("part_b"), part_b).expect("Failed to open part B");

    // create a new FileMem object
    let mut output = netcdf::create_with("new.nc", netcdf::Options::DISKLESS)
        .expect("Failed to create output file");

    for file in [&file_a, &file_b] {
        // Copy dimensions
        for dim in file.dimensions() {
            let name = dim.name();
            let len = dim.len();
            if output.dimension(&name).is_none() {
                if len == 0 {
                    output.add_unlimited_dimension(&name).unwrap();
                } else {
                    output.add_dimension(&name, len).unwrap();
                }
            }
        }

        // Copy variables
        for var in file.variables() {
            let name = var.name();
            if output.variable(&name).is_none() {
                let var_type = var.vartype();
                let dim_ids: Vec<DimensionIdentifier> =
                    var.dimensions().into_iter().map(|d| d.identifier()).collect();
                let mut new_var = match output.add_variable_from_identifiers_with_type(&name, &dim_ids, &var_type) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to add variable '{}': {}", name, e);
                        // Skip this variable on error
                        continue;
                    }
                };

                // Copy variable data
                let data: Vec<u8> = var.get_raw_values(netcdf::Extents::All).unwrap();
                new_var.put_values(&data, netcdf::Extents::All).unwrap();
            }
        }

        // Copy attributes
        for attr in file.attributes() {
            output.add_attribute::<AttributeValue>(&attr.name(), attr.value().unwrap()).unwrap();
        }
    }

    // turn the output into raw binary data
    output
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
