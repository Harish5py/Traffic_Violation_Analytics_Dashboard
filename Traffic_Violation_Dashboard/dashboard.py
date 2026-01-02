import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine


@st.cache_resource(ttl=300)
def get_conn():
    return create_engine("mysql+mysqlconnector://" \
    "4WbWyupWCd3hqWf.root:tNYOux4d2tMVPa1T@" \
    "gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/Traffic_Violation_DB")

@st.cache_data(ttl=300)
def execute_query(query):
    df = pd.read_sql(query, con=get_conn())
    return df

@st.cache_data(ttl=300)
def fetch_eda():
    return {
        "query1": {"header":"What are the most common violations?", "data":execute_query(
            """select Violation_Type, count(*) as Violation_Count from Traffic_Violation
            group by Violation_Type order by Violation_Count desc limit 20;""")},
        "query2": {"header":"Which areas or coordinates have the highest traffic incidents?", "data":execute_query(
            """select Location, Latitude, Longitude, count(*) as No_of_Traffic_incident from Traffic_Violation 
            group by Location, Latitude, Longitude order by No_of_Traffic_incident desc limit 1;""")},
        "query3": {"header":"Do certain demographics correlate with specific violation types?", "data":execute_query(
            """select Race, Gender, Violation_Type, count(*) as Count from Traffic_Violation
            group by Race, Gender, Violation_Type order by Count desc limit 20;""")},
        "query4": {"header":"How does violation frequency vary by time of day, weekday, or month?", "data":execute_query(
            """select extract(hour from Time_Of_Stop) as hour_of_day, dayname(Date_Of_Stop) as weekday,
            monthname(Date_Of_Stop) as month, count(*) as violation_count from Traffic_Violation
            where Date_Of_Stop is not null and Time_Of_Stop is not null group by extract(hour from Time_Of_Stop), 
            dayname(Date_Of_Stop), monthname(Date_Of_Stop) order by violation_count desc limit 20;""")},
        "query5": {"header":"What types of vehicles are most often involved in violations?", "data": execute_query(
            """select VehicleType, count(*) as Violation_Count from Traffic_Violation where VehicleType is not null
            group by vehicleType order by Violation_Count desc limit 20; """)},
        "query6": {"header":"How often do violations involve accidents, injuries, or vehicle damage?", "data":execute_query(
            """select Violation_Type, sum(Accident) as accident_count, sum(Personal_Injury) as injury_count,
            sum(Property_Damage) as damage_count, count(*) as total_violations, 
            round(sum(Accident) * 100.0 / count(*), 2) as pct_accident, 
            round(sum(Personal_Injury) * 100.0 / count(*), 2) as pct_injury,
            round(sum(Property_Damage) * 100.0 / count(*), 2) as pct_damage
            from Traffic_Violation group by Violation_Type order by total_violations desc limit 20;""")}
    }

@st.cache_data(ttl=300)
def fetch_metrics():
    return {
        "metric1": {
            "header":"Total Violations", 
            "data":execute_query("select count(*) from Traffic_Violation;").values[0]
        },
        "metric2": {
            "header":"Violations involving Accidents", 
            "data":execute_query("select sum(Accident) from Traffic_Violation;").values[0]
        },
        "metric3": {
            "header":"High-risk Zones", 
            "data":execute_query("""select Location from Traffic_Violation 
                                 where Accident=1 group by Location 
                                 order by count(*) desc limit 1;""").values[0][0]
        },
        "metric4": {
            "header":"Most frequently sited vehicle Makes/Models", 
            "data":execute_query("""select Make from Traffic_Violation
                                 group by Make order by count(*) desc limit 1;""").values[0][0]
        },
    }


st.set_page_config(
    page_title="Traffic Violation Analytics Dashboard",
    page_icon="📊",
    # layout="wide"
)


year_wise_df = execute_query("""select Year(Date_Of_Stop) as `Year`, 
                                        count(*) as Violations,
                                        sum(Accident) as Accidents, 
                                        sum(Personal_Injury) as Personal_Injuries, 
                                        sum(Property_Damage) as Property_Damages
                                        from Traffic_Violation
                                        group by Year(Date_Of_Stop)
                                        order by `Year`;""")
top_10_model_df = execute_query("""select Make, count(*) as Violations
                            from Traffic_Violation
                            group by Make
                            order by Violations desc limit 10;""")
eda_data = fetch_eda()
metrics_data = fetch_metrics()


st.title("Welcome to Traffic Violation Analytics Dashboard")
st.header("Overview")
col = st.columns(2)
for ind, metric in enumerate(list(metrics_data)[:2]):
    col[ind].metric(
        metrics_data.get(metric, {}).get("header", ""), 
        value=metrics_data.get(metric, {}).get("data", 0), 
        border=True)
for ind, metric in enumerate(list(metrics_data)[2:]):
    st.metric(
        metrics_data.get(metric, {}).get("header", ""), 
        value=metrics_data.get(metric, {}).get("data", 0), 
        border=True)
st.divider()


bars = plt.bar(top_10_model_df.Make, top_10_model_df.Violations, color='skyblue')
plt.xlabel("Make")
plt.ylabel("Violations")
plt.title("Make wise Violations")
plt.xticks(rotation=45)
st.pyplot(plt)
st.divider()


columns = st.multiselect(
    "Select columns to compare:",
    options=year_wise_df.columns[1:],  # Exclude Year
    default=["Accidents"]
)
if columns:
    plt.figure(figsize=(8, 5))
    
    for col in columns:
        plt.plot(year_wise_df["Year"], year_wise_df[col], marker='o', label=col)
    plt.xlabel("Year")
    plt.ylabel("Value")
    plt.title("Year-wise Comparison")
    plt.legend()
    plt.grid(True)
    st.pyplot(plt)
else:
    st.warning("Please select at least one column.")
st.divider()


violation_df = execute_query("select * from Traffic_Violation limit 1000;")
st.header("Select options to filter the Traffic Violation data")
col = st.columns(2)
selected_col = col[0].selectbox(
    "Select a column to filter", 
    ["Date_Of_Stop", "Location", "VehicleType", "Gender", "Race", "Violation_Type"], 
    None)
selected_val = col[1].selectbox(
    "Select a value to filter", 
    violation_df[selected_col].unique() if selected_col else [], 
    None)
st.dataframe(
    violation_df.loc[
        violation_df[selected_col]==selected_val, :
    ].reset_index(drop=True) if selected_col else pd.DataFrame())
st.divider()


st.header("EDA")
for key in eda_data.keys():
    st.subheader(eda_data.get(key, {}).get("header", ""))
    # with st.expander("Click to view the data"):
    st.dataframe(eda_data.get(key, {}).get("data", pd.DataFrame()), hide_index=True)
st.divider()
