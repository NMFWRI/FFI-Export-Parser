Author: Corey Beinhart
Contact: corey@nmhu.edu

This python program functions to migrate FFI data stored in XML files into our PostgreSQL database.

The code will parse XML files using the FFIFile and XMLFrame classes. These are built out to be fairly generalized in
the case that anyone wants to expand on this functionality. FFIFile is basically a collection of XMLFrames that also 
contains some metadata regarding the file. 

As it goes now, it will iterate through a directory and write each XML file into the database. I'd like to expand this
such that the code can also rollback changes and run individually specified files. I'd also like to write a listener
for the SharePoint site from which these FFI admin exports are downloaded from.

Functionality for duplicate file checking (that is, if a file has already been written into the database) needs to be 
modified. The way I handled it initially is insufficient and will most certainly create some issues down the line.

Yes! You too can use this for your own purposes! If you use FFI and want to use this parser for your own database and 
analysis, all you have to do is provide a config.ini file.
The config file is only setup so far for PostgreSQL connections, but the code can very easily be changed to use a 
different database connection type. 

For Postgres connections, the format of the config file should be:
[POSTGRESQL]
type = postgresql
driver = postgresql+psycopg2
HOST = 
DATABASE = 
UID = 
PWD = 
where all of the blank fields should be filled out as the is relevant by the user, since it will change based on whether
or not the connection is local or remote.

If you have any questions or requests, please reach out to me.