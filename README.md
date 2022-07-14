
# descriptive_stats tool 

The tool is to be used on regulated study data to provide the required analysis for missingness on top of the profiling that is 
providing missingness percentages and linkage issues.

The tools output generates one QA table in the public schema of the database you've pointed the tool. 
The QA table's name will be similar to **QA_DESCRIPTIVE_STATS_SCHEMA_ANALYZED_YYYYMMDD**

- This tools focus is to group variables in each column of a table and provide descriptive aka summary statistics on each group by count.
- Nulls are also counted in this by grouping all nulls with in a column and providing the output described in a later section.
- The tool does not provide analysis on tables that are completely null and do not have any categorical data.

**The tool is used to provide the following output:**
- group by count of distinct variables within each field/column of a table
- percentage of the group by count / total count of all objects in the column
- mean of the group by counts
- median of the group by counts
- standard deviation of the group by counts which shows the spread of counts
- maximum group by count
- minimum group by count


# Getting started

Depending where your GitHub folder is on your machine you can pull and get the latest version of the tool.
The location of your GitHub directories will vary.
- Depending on where this tool is located on your machine. This will dictate the directory for where the script descriptive_stats.py lives.
- Make note of the directory and copy the file path for ease of use later. 


**Ensure that python is installed on your machince and you are comfortable with creating a virtual environment to install the dependencies.**

## Install dependencies

Before using the the tool it is advised to create a virtual environment to package all libraries needed
for running the tool.
- **ensure you're in the correct directory where the tool lives**
- > source venv/bin/activate this activates the virtual environment
- > $ pip install -r requirements

You only have to download the dependencies once in your virtual environment.
Now after all dependencies have been successfully downloaded you're ready to run the tool.

# RUN THE TOOL


