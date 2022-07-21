This repository contains a tool capable of parsing SQL statements, 
extracting owner and object names, including their associated object 
type.

Under the scr folder, you can find readobj.py which is the main module 
used to parse SQL statements. The data passed into this file is under 
config.py.

Addtionally, the app.py module in the root directory of the project can 
deploy the project as a locally hosted API. Further work will be done so 
that users can connect to the network and pass into SQL statements 
externally.
