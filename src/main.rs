use rocket::response::status;

#[macro_use] extern crate rocket;

#[post("/part_a?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_a(name: &str, input: &[u8]) -> status::Accepted<String> {
    let file = netcdf::open_mem(Some(name), input).expect("fail");
    let var = &file.variable("Hexahedron_Vertex").expect("Could not find variable 'data'");
    let data_i32 = var.get_value::<i32, _>([1, 1]).expect("fail");
    status::Accepted(format!("id: '{}'", data_i32))
}

#[post("/part_b?<name>", format = "application/x-netcdf", data = "<input>")]
fn part_b(name: &str, input: Vec<u8>) -> &'static str {
    "Hello, world!"
}

#[get("/read?<name>")]
fn read(name: &str) -> &str {
    return name;
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![part_a, part_b, read])
}
