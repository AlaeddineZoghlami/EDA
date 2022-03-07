### Using Pandas Profiling & Sweetviz in Streamlit with Streamlit Components
+ How to use pandas profiling and sweetviz in streamlit

#### Simple EDA App
+ Requirements
	- Streamlit version 0.63 and up
	- pandas-profiling
	- sweetviz
	- streamlit-pandas-profiling


#### Streamlit Components Types
![](images/streamlit_component_types_jcharistech.png)

#### More on Components
(https://docs.streamlit.io/en/stable/develop_streamlit_components.html)




## installing local sweetviz
```
TO INSTALL LOCAL SWEETVIZ
cd sweetviz
pip install -e .



TO USE THE APP
streamlit run app.py --server.maxUploadSize=5000




IF TABLE IS TO LARGE > 1M .. 
EXPORT RANDOM SAMPLE TO DRIVE AND THEN DOWNLOAD IT (CSV)
BIGQUERY TO CSV 
#standardSQL
SELECT word
FROM `PROJECT.DATASET.TABLE`
WHERE RAND() < 1000000/(SELECT COUNT(*) FROM `PROJECT.DATASET.TABLE`)
```
