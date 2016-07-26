# Data Aansluitpunt voor Emissie-Imissie (EI) toets
This tool consists of two parts:
- a small web application ('data aansluitpunt') that creates web services to retrieve data using get requests from a MongoDB database. This database contains emission info (3-year averages, locations, and auxiliary information) for use in the EI-toets.
- a standalone python script (Compute_3YearAvg_DDL.py) that reads water quality data from the Rijkswaterstaat DDL and obtains 'normen' from the RIVM-normendatabase, and using these two data sources computes the 3-year average. The results are stored in a mongoDB database that can be queried using the 'data-aansluitpunt' web application.

In order to run both the data_aansluitpunt and the script, MongoDB needs to be installed. The required python packages are listed in requirements.txt.



