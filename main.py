import scripts
import streamlit as st
import plotly.express as px
import pandas as pd

FILENAME='retention_snapshots.csv'
DUNE_KEY=st.secrets['dune_key']
snapshots=pd.read_csv(FILENAME)
snapshots['snapshots']=snapshots['snapshots'].apply(scripts.string_to_dict)


st.title('Lido Dune API use cases')

st.header('Use case 1: AAVE liquidations')
st.write('We need to track how much of our stETH might be under liquidation on AAVE, in order to do this we need to get all wallets with stETH as collateral. This is pretty complicated process to take this info on-chain, but it is really easy with Dune API.')
st.write("**You could look through result [here](https://deepnote.com/workspace/greg-s-b1d4-1ea5fde6-7b86-41cd-8442-2f1e4791af53/project/Aave-risk-monitoring-Dune-API-7fc43e6f-8333-4988-9a28-076ea5c00c3f/%2Fnotebook.ipynb)**")
st.write('More on [Lido risk assessment](https://blog.lido.fi/lido-stassets-collateral-risk-monitoring/)')

st.header('Use case 2: Retention snapshots')
st.write('Currently, we could calculate retention only for the current day, but we also would like to track changes in retention, so we create snapshots of state. We use execution id functionality, we execute the query and save execution id in a form of «date»: «execution_id» thus we got snapshot info and could get it for analysis once we need it.')
st.write('More on [Lido approach to retention calculation](https://blog.lido.fi/analysis-of-steth-user-behaviour-patterns/)')
st.write('Feel free to try this:')
available_dates=scripts.get_available_dates(FILENAME)

date=st.selectbox("Select available date",available_dates)
if st.button('Get retention snapshots'):

    active_wallets_share = px.bar(scripts.get_query_result(snapshots.iloc[0, 2][str(date)], DUNE_KEY), x='client_size', y=['active_wallets_share'],
           title=snapshots.iloc[0, 0])
    st.plotly_chart(active_wallets_share, use_container_width=True)
    passive_wallets_share=px.bar(scripts.get_query_result(snapshots.iloc[1, 2][str(date)],DUNE_KEY), x='client_size', y=['passive_wallets_share'],
           title=snapshots.iloc[1, 0])
    st.plotly_chart(passive_wallets_share, use_container_width=True)
    strategies_more_10=px.bar(scripts.get_query_result(snapshots.iloc[2, 2][str(date)], DUNE_KEY), x='strategy', y=['share'],
           title=snapshots.iloc[2, 0])
    st.plotly_chart(strategies_more_10, use_container_width=True)
    strategies_less_10=px.bar(scripts.get_query_result(snapshots.iloc[3, 2][str(date)], DUNE_KEY), x='strategy', y=['share'],
           title=snapshots.iloc[3, 0])
    st.plotly_chart(strategies_less_10, use_container_width=True)
    source_more_10=px.bar(scripts.get_query_result(snapshots.iloc[4, 2][str(date)], DUNE_KEY), x='strategy', y=['share'],
           title=snapshots.iloc[4, 0])
    st.plotly_chart(source_more_10, use_container_width=True)
    source_less_10=px.bar(scripts.get_query_result(snapshots.iloc[5, 2][str(date)], DUNE_KEY), x='strategy', y=['share'],
           title=snapshots.iloc[5, 0])
    st.plotly_chart(source_less_10, use_container_width=True)


st.header('Use case 3: Cross-chain analysis')
st.write('Since Lido presented in many chains we need to analyze our performance in each chain, and with dune API we are finally able to gather all info into one chart (for chains which are supported by dune we take info from dune API, for chains which are not supported we use own tools, but in the end everything is putted into one chart for cross-chain analysis)')
total_tvl=scripts.create_total_tvl(DUNE_KEY)
tvl_chart=px.bar(total_tvl,x='date',y='tvl',color='network',title='Crosschain TVL')
st.plotly_chart(tvl_chart,use_container_width=True)

total_depositors=scripts.create_total_depositor_table(DUNE_KEY)
depositors_chart=px.bar(total_depositors,x='date',y=['ETH_depositors','MATIC_depositors','sol_depositors','ksm_depositors','dot_depositors'])
st.plotly_chart(depositors_chart,use_container_width=True)