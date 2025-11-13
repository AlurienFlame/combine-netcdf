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
## Code Structure
`main.rs` contains three endpoints, and several functions for merging files.
- `/part_a` and `/part_b` accept file uploads and store them in a map based on name.
- `/read` merges the two files in memory and sends the result to the client. It calls:
    - `merge_parts`, which takes the binary data of the two files and opens them in memory, before passing them to:
        - `merge_files`, which reads the files' metadata and copies one onto the other, using several helper functions:
            - `merge_dims`
            - `merge_attrs`
            - `merge_var_definitions`
            - `merge_var_data`
## The Approach
Essentially, when the user hits the read endpoint, we do the following: make an empty NetCDF file in memory, then copy over each thing from the first file, and then from the second. In the case of conflicts, the second file's copying quietly fails. Now, it may have been simpler to just copy variables from the second file to a duplicate of the first - likely far simpler - but by the time I realized that, I already had this approach mostly working, so it was easier to finish that up. The result is definitely over-engineered, but it's also a lot more explicit about what data gets carried over, so there's less chance of discreet format or metadata details on the files causing confusing bugs.
## Handling Parallel Requests
Currently, we store uploaded files in a map inside a Mutex, which avoids parallelism issues by not allowing it at all. This utterly fails to scale. Furthermore, the netcdf_sys library is not thread-safe, which creates another bottleneck.

The storage issue could probably be solved pretty straightforwardly by using something like a DashMap, but the other problem is less tractable.

I would probably approach it by wrapping the netcdf_sys stuff in some kind of encompassing interface that enforces safety, similar to what the `netcdf` library does. I'd have to look more into where exactly the thread-safety issues arise, but I suspect that keeping track of which files are in use and preventing concurrent access to them would help.
