## How to Run
## Handling Parallel Requests
## Spec
make a simple rust server that can combine NetCDF files in memory.
- It should accept POSTs to /part_a?name and /part_b?name with a netcdf file in the body
- It should accept a GET to /read?name which should return the combined netcdf.
- You are welcome to use other frameworks, but personally I like https://rocket.rs/
- You are welcome to use LLMs and any libraries you would like. It wouldn't be fair to give this assignment without LLMs, but they're helpful with rust regardless of experience level.
- It is critical that the file does not hit disk at any point, even temporarily.
- It should also include a note in the readme explaining how you would make it handle parallel requests correctly. Hint: this is much harder than it seems, so I encourage you to put thought into this.

My criteria for success:
- It works
- It follows all the requirements (ie, does not touch disk)
- Your overall architecture and code style looks sane. And I only mean sane: it doesn't need to be anything close to beautiful.