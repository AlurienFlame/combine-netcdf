use rocket::response::status;
use rocket::State;
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

// static mut PART_A: Option<Vec<u8>> = None;
// static mut PART_B: Option<Vec<u8>> = None;

#[post("/part_a?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_a(name: String, input: Vec<u8>, state: &State<AppState>) -> status::Accepted<String> {
    // Dig up the relevant part of state, or create it if necessary
    let mut storage = state.storage.lock().unwrap();
    let entry = storage.entry(name.clone()).or_insert_with(|| Parts { part_a: None, part_b: None });
    // Update state with the uploaded file
    entry.part_a = Some(input);
    status::Accepted(format!("recieved: '{}'", name))
}

#[post("/part_b?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_b(name: String, input: Vec<u8>, state: &State<AppState>) -> status::Accepted<String> {
    // Dig up the relevant part of state, or create it if necessary
    let mut storage = state.storage.lock().unwrap();
    let entry = storage.entry(name.clone()).or_insert_with(|| Parts { part_a: None, part_b: None });
    // Update state with the uploaded file
    entry.part_b = Some(input);
    status::Accepted(format!("recieved: '{}'", name))
}

#[get("/read?<name>")]
fn read(name: &str, state: &State<AppState>) -> Vec<u8> {
    let storage = state.storage.lock().unwrap();
    let entry = storage.get(name).expect("fail");
    return entry.part_a.clone().unwrap();
    // let file = netcdf::open_mem(Some(name), input).expect("fail");
    // let var = &file.variable("Hexahedron_Vertex").expect("Could not find variable 'data'");
    // let data_i32 = var.get_value::<i32, _>([1, 1]).expect("fail");
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![part_a, part_b, read])
        .manage(AppState {
            storage: Mutex::new(HashMap::new()),
        })
}
