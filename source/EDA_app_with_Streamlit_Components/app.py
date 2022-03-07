import streamlit as st
import pandas as pd
import codecs
from pandas_profiling import ProfileReport
import streamlit.components.v1 as components
from streamlit_pandas_profiling import st_profile_report
from streamlit.report_thread import REPORT_CONTEXT_ATTR_NAME
from threading import current_thread
from contextlib import contextmanager
from io import StringIO
import sys
import logging
import time
import sweetviz as sv
import SessionState  # Assuming SessionState.py lives on this folder
from google.oauth2 import service_account
from google.cloud import bigquery

client = bigquery.Client()


### this part below begins for terminal output
@contextmanager
def st_redirect(src, dst):
    placeholder = st.empty()
    output_func = getattr(placeholder, dst)

    with StringIO() as buffer:
        old_write = src.write

        def new_write(b):
            if getattr(current_thread(), REPORT_CONTEXT_ATTR_NAME, None):
                buffer.write(b + '')
                output_func(buffer.getvalue() + '')
            else:
                old_write(b)

        try:
            src.write = new_write
            yield
        finally:
            src.write = old_write


@contextmanager
def st_stdout(dst):
    "this will show the prints"
    with st_redirect(sys.stdout, dst):
        yield


@contextmanager
def st_stderr(dst):
    "This will show the logging"
    with st_redirect(sys.stderr, dst):
        yield


def demo_function():
    """
    Just a sample function to show how it works.
    :return:
    """
    for i in range(10):
        logging.warning(f'Counting... {i}')
        time.sleep(2)
        print('Time out...')

## this part above ends the terminal output part

st.set_page_config(
     page_title="Simple EDA App",
     page_icon=":bat:",
     layout="wide",
     initial_sidebar_state="expanded",
 )


def st_display_sweetviz(report_html, width=1000, height=500):
    report_file = codecs.open(report_html, 'r')
    page = report_file.read()
    components.html(page, width=width, height=height, scrolling=True)




# def random_sample_select(number_rows, sample_percentage_param):
    #     # random sample
    #     desired_sample_size = int(np.round((number_rows * sample_percentage_param) / 100))
    #     skip = sorted(random.sample(range(number_rows), number_rows - desired_sample_size))
#     return skip

def expander_variable_type(df, comment="See Columns"):
    with st.sidebar.expander(comment):
        for df_col in df.columns:
            # if (str(df[df_col].dtypes)=="float64") or (str(df[df_col].dtypes)=="int64") :
            st.write(df_col + ": " + str(df[df_col].dtypes))
            col1, col2 = st.columns(2)
            with col1:
                to_numeric = st.checkbox("force to numeric", key=df_col + "_col1" + comment)
            with col2:
                to_string = st.checkbox("force to string", key=df_col + "_col2" + comment)

            if to_numeric:
                df[df_col] = pd.to_numeric(df[df_col], errors='coerce')
            if to_string:
                df[df_col] = df[df_col].astype(str)


def main():
    """A Simple EDA App with Streamlit Components"""
    session = SessionState.get(run_id=0)
    if st.sidebar.button("Reset"):
        session.run_id += 1

    menu = ["Home", "Pandas Profile","Pandas Profile Compare" ,"Sweetviz", "Sweetviz Compare"]
    choice = st.sidebar.selectbox("Menu", menu)
    connector = ["local csv", "bigquery api"]
    choice_connector = st.sidebar.selectbox("Connector", connector)
    restart_force = st.sidebar.button("Force restart")
    if restart_force:
        st.stop()




     # st.slider("Slide me!", 0, 100, key=session.run_id)
#    sample_percentage = st.sidebar.slider('input too large ?' ' select Random Sample :  ', 0, 100, 100)
#    sample_percentage = int(sample_percentage)

    # pandas profiling comparre
    if choice == "Pandas Profile Compare":
        minimal_checkbox = st.sidebar.checkbox("minimal")
        df_1 = None
        df_2 = None
        ##local csv connector
        if choice_connector=="local csv":
            sep_param_sv_1 = st.sidebar.selectbox("file 1 : separteur csv", (',', ';', '\t'))
            sep_param_sv_2 = st.sidebar.selectbox("file 2 : separteur csv", (',', ';', '\t'))
            same_schema_button = st.sidebar.checkbox('Apply Same Schema')
            st.subheader("Compare between different sources with Pandas Profiling")
            data_file_1 = st.file_uploader("Upload CSV 1", type=['csv'], key="x1"+choice)
            data_file_2 = st.file_uploader("Upload CSV 2", type=['csv'], key="x2"+choice)
            if (data_file_1 is not None) & (data_file_2 is not None):
                df_1 = pd.read_csv(data_file_1, sep=sep_param_sv_1)
                df_2 = pd.read_csv(data_file_2, sep=sep_param_sv_2)
            if same_schema_button:
            # applying prd schema's on dev
                schema_df = pd.DataFrame(df_1.dtypes).reset_index()
                schema_df.columns = ["var_name", "var_type"]
                schema_df["var_type"] = schema_df["var_type"].astype("string")
                schema_df = schema_df.replace({'var_type': {'int64': 'float64'}})
                for i in range(0, len(schema_df)):
                    df_2[df_2.columns[i]] = df_2[df_2.columns[i]].astype(
                        schema_df["var_type"][i])

        ##cloud bigquery connector
        elif choice_connector == "bigquery api":

            with st.form(key='my_form'):
                label_1 = "write your first sql query"
                # Using the "with" syntax
                sql_query_1 = st.text_area(label_1, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50,
                                           key="query_1")
                label_2 = "write your second sql query"
                # Using the "with" syntax
                sql_query_2 = st.text_area(label_2, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50,
                                           key="query_2")
                submit_button_query_pandas_both = st.form_submit_button(label='RUN QUERIES')
            if submit_button_query_pandas_both:
                query_job_pandas_1 = client.query(sql_query_1)
                df_1 = query_job_pandas_1.result().to_dataframe()
                query_job_pandas_2 = client.query(sql_query_2)
                df_2 = query_job_pandas_2.result().to_dataframe()

        else:
            pass



        if (df_1 is not None) and (df_2 is not None):
            st.dataframe(df_1.head())
            st.dataframe(df_2.head())
            # expander_variable_type(df_1,"See Columns df 1")
            # expander_variable_type(df_2, "See Columns df 2")
            col1, col2 = st.columns(2)
            with col1:
                profile_1 = ProfileReport(df_1, minimal=minimal_checkbox, progress_bar=True, infer_dtypes=True)
                st_profile_report(profile_1)
            with col2:
                profile_2 = ProfileReport(df_2, minimal=minimal_checkbox, progress_bar=True, infer_dtypes=True)
                st_profile_report(profile_2)

    # pandas profiling
    elif choice == "Pandas Profile":
        minimal_checkbox = st.sidebar.checkbox("minimal")
        df = None
        ##local csv connector
        if choice_connector=="local csv":
            sep_param_sv = st.sidebar.selectbox("separteur csv", (',', ';', '\t'))
            st.subheader("Automated EDA with Pandas Profile")
            data_file = st.file_uploader("Upload CSV", type=['csv'])
            if data_file is not None:
                df = pd.read_csv(data_file, sep=sep_param_sv)


        ##cloud bigquery connector
        elif choice_connector == "bigquery api":
            label = "write your sql query"
            # Using the "with" syntax
            with st.form(key='my_form'):
                sql_query  = st.text_area(label, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50 )
                submit_button_query = st.form_submit_button(label='RUN QUERY')
            if submit_button_query:
                query_job = client.query(sql_query)
                df = query_job.result().to_dataframe()
        else:
            pass

        # pandas profiling
        if df is not None:
            st.dataframe(df.head())
            # expander_variable_type(df)

            profile = ProfileReport(df, minimal=minimal_checkbox, progress_bar=True, infer_dtypes=True)
            st_profile_report(profile)






    # sweetviz
    elif choice == "Sweetviz":
        df = None
        pairwise_analysis_param_sv = st.sidebar.selectbox("Pairwise for SweetVZ", ('off', 'auto', 'on'))
        ##local csv connector
        if choice_connector=="local csv":
            sep_param_sv = st.sidebar.selectbox("separteur csv", (',', ';', '\t'))
            st.subheader("Automated EDA with Sweetviz")
            data_file = st.file_uploader("Upload CSV", type=['csv'])
            if data_file is not None:
                df = pd.read_csv(data_file, sep=sep_param_sv)

            ##cloud bigquery connector
        elif choice_connector == "bigquery api":
            label = "write your sql query"
            # Using the "with" syntax
            with st.form(key='my_form'):
                sql_query = st.text_area(label, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50 )
                submit_button_query = st.form_submit_button(label='RUN QUERY')
            if submit_button_query:
                query_job = client.query(sql_query)
                df = query_job.result().to_dataframe()
        else:
            pass

        # sweetviz
        if df is not None:
            st.dataframe(df.head())
            # expander_variable_type(df)
            # if st.button("Generate Sweetviz Report"):
            report = sv.analyze(df,  pairwise_analysis=pairwise_analysis_param_sv)
            report.show_html()
            st_display_sweetviz("SWEETVIZ_REPORT.html")



    elif choice == "Sweetviz Compare":
        df_1 = None
        df_2 = None
        pairwise_analysis_param_sv = st.sidebar.selectbox("Pairwise for SweetVZ", ('off', 'auto', 'on'))
        if choice_connector=="local csv":
            sep_param_sv_1 = st.sidebar.selectbox("file 1 : separteur csv", (',', ';', '\t'))
            sep_param_sv_2 = st.sidebar.selectbox("file 2 : separteur csv", (',', ';', '\t'))
            same_schema_button = st.sidebar.checkbox('Apply Same Schema')
            st.subheader("Compare between different sources with sweetviz")
            data_file_1 = st.file_uploader("Upload CSV prd", type=['csv'], key="x1"+choice)
            data_file_2 = st.file_uploader("Upload CSV dev", type=['csv'], key="x2"+choice)
            if (data_file_1 is not None) & (data_file_2 is not None):
                df_1 = pd.read_csv(data_file_1, sep=sep_param_sv_1)
                df_2 = pd.read_csv(data_file_2, sep=sep_param_sv_2)
                if same_schema_button:
                # applying prd schema's on dev
                    schema_df = pd.DataFrame(df_1.dtypes).reset_index()
                    schema_df.columns = ["var_name", "var_type"]
                    schema_df["var_type"] = schema_df["var_type"].astype("string")
                    schema_df = schema_df.replace({'var_type': {'int64': 'float64'}})
                    for i in range(0, len(schema_df)):
                        df_2[df_2.columns[i]] = df_2[df_2.columns[i]].astype(
                            schema_df["var_type"][i])

            ##cloud bigquery connector
        elif choice_connector == "bigquery api":

            with st.form(key='my_form'):
                label_1 = "write your first sql query"
                # Using the "with" syntax
                sql_query_1 = st.text_area(label_1, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50,
                                            key="query_1")
                label_2 = "write your second sql query"
                # Using the "with" syntax
                sql_query_2 = st.text_area(label_2, value="select * from big-data-dev-lmfr.SUPPLY_FRANCO.df_depol_s1 limit 200", height= 50,
                                            key="query_2")
                submit_button_query_sweetviz_both = st.form_submit_button(label='RUN QUERIES')
            if submit_button_query_sweetviz_both:
                query_job_sweetviz_1 = client.query(sql_query_1)
                df_1 = query_job_sweetviz_1.result().to_dataframe()
                query_job_sweetviz_2 = client.query(sql_query_2)
                df_2 = query_job_sweetviz_2.result().to_dataframe()

        else:
            pass


        if (df_1 is not None) and (df_2 is not None):
            st.dataframe(df_1.head())
            st.dataframe(df_2.head())
            # expander_variable_type(df_1,"See Columns df 1")
            # expander_variable_type(df_2, "See Columns df 2")
            # Normal Workflow
            report = sv.compare([df_1, "df 1"], [df_2, "df 2"], pairwise_analysis=pairwise_analysis_param_sv)
            report.show_html()
            st_display_sweetviz("SWEETVIZ_REPORT.html")


    else:
        st.subheader("")
        html_temp = """
		<div style="background-color:green;padding:10px;border-radius:10px">
		<h1 style="color:white;text-align:center;">Welcome to the EDA APP</h1>
		</div>
		"""

        # components.html("<p style='color:red;'> Streamlit Components is Awesome</p>")
        components.html(html_temp)
        code_sql_ex = """
        #standardSQL
            SELECT * 
            FROM `lmfr-ddp-ods-prd.delay_elasticity.elasticity_result`
            WHERE RAND() < 30000/(SELECT COUNT(*) FROM `lmfr-ddp-ods-prd.delay_elasticity.elasticity_result`)
        """
        st.caption('Table too large to load/process? ... Think about sampling it like the example below')
        st.code(code_sql_ex, language="sql")

if __name__ == '__main__':
    # terminal output  in app

        #with st.expander("terminal output"):
        with st_stdout("success"), st_stderr("code"):
                main()




