# CIHM-METS-app
Command-line tool for generating/manipulating components of METS records

Not much written about module yet. This is based on some internal functionalty with history, and is being made available on GitHub.

This command-line tool is called from an automated tool used by the production team when generating or updating AIPs, and read from files which they saved and generates the metadata components (item dmdSec, component labels and item labels).  This tool normally writes to CouchDB, but can also output data to console during development/debugging.

