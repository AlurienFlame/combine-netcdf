#[macro_use] extern crate rocket;

#[get("/part_a?<name>")]
fn part_a(name: &str) -> &'static str {
    "Hello, world!"
}

#[get("/part_b?<name>")]
fn part_b(name: &str) -> &'static str {
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
