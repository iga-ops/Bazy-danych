Section 1: The contents of the replication documentation

Folder Analysis_Data:
original file with file name changed 0_DOLNOSlĄSKIE.csv.
extracted parts of the data from the original file used for the further analysis saved into .csv files
the graphic interpretation of the extracted data in form of bar diagrams and histograms used to compare the quantity and independencies between the variables
Folder Command_Files:
command file Command_file.ipynb.
Folder Douments:
file readme.md.
The_Data_Appendix.md
Folder Original_Data:
original file 0_DOLNOS╠üLA╠ĘSKIE.csv;
original file with file name changed 0_DOLNOSlĄSKIE.csv.
Metadata folder:
The_Metadata_Guide.md

Section 2: Modifications made to importable data files
Copy of the original file 0_DOLNOS╠üLA╠ĘSKIE.csv has been saved as 13_WARMIŃSKO-MAZURSKIE.csv because of the special characters hindering the comprehensive meaning of the file origin and contents.

Section 3: Instructions for replicating the study
The softaware necessary to run the command file used for data processing and analysis conducted for the study consists of Python3 with the additional imports from matplotlib, pandas and numpy. The hierarchy of the folders should be compatible with the imports and exports run from the command file, such as the location of the Analysis_Data folder and the Original_Data folder containing the original file 13_WARMIŃSKO-MAZURSKIE.csv crucial to the data processing (source of all the data used for the study).

The only command file provided contains all the processings of the data which ensured the results of the study. Firstly, the data is cleaned from empty records or records that do not contain the adequate data used for particular phase of the analysis and saved into separate files in the Analysis_Data folder for any further possibility of development. The fils are then the basis for the graphic interpretation of the variables and their codependencies.
