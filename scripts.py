import pandas as pd
import requests
import time
from datetime import datetime
import json


def get_dune_data(query_id,dune_key):
    query_link=f'https://api.dune.com/api/v1/query/{query_id}/execute'
    headers = {'x-dune-api-key': dune_key}
    data = '{}'
    query_execution_link= requests.post(query_link, headers=headers, data=data)
    query_execution_id=query_execution_link.json()['execution_id']
    status_link=f'https://api.dune.com/api/v1/execution/{query_execution_id}/status'
    status_query = requests.get(status_link, headers=headers, data=data)
    while status_query.json()['state'] != 'QUERY_STATE_COMPLETED' :
        time.sleep(10)
        status_query = requests.get(status_link, headers=headers, data=data)
        print(status_query.json())
    result_link=f'https://api.dune.com/api/v1/execution/{query_execution_id}/results'
    result_query = requests.get(result_link, headers=headers, data=data)
    results_df=pd.DataFrame(result_query.json()['result']['rows'])
    return results_df

def get_execution_id(query_id,dune_key):
    query_link=f'https://api.dune.com/api/v1/query/{query_id}/execute'
    headers = {'x-dune-api-key': dune_key}
    data = '{}'
    query_execution_link= requests.post(query_link, headers=headers, data=data)
    query_execution_id=query_execution_link.json()['execution_id']

    return query_execution_id


def get_query_result(query_execution_id,dune_key):
    headers = {'x-dune-api-key': dune_key}
    data = '{}'
    status_link=f'https://api.dune.com/api/v1/execution/{query_execution_id}/status'
    status_query = requests.get(status_link, headers=headers, data=data)
    while status_query.json()['state'] != 'QUERY_STATE_COMPLETED' :
        time.sleep(10)
        status_query = requests.get(status_link, headers=headers, data=data)
        print(status_query.json())
    result_link=f'https://api.dune.com/api/v1/execution/{query_execution_id}/results'
    result_query = requests.get(result_link, headers=headers, data=data)
    results_df=pd.DataFrame(result_query.json()['result']['rows'])
    return results_df

def add_snapshots(file_name, dune_key):
    snapshots=pd.read_csv(file_name)
    snapshots['snapshots']=snapshots['snapshots'].apply(string_to_dict)
    for query_id in range(len(snapshots)):
        id=get_execution_id(snapshots.iloc[query_id,1],dune_key)
        date=datetime.today().strftime('%Y-%m-%d')
        snapshots.iloc[query_id,2][date]=id
    snapshots.to_csv(file_name, encoding='utf-8',index=False)


def string_to_dict(dict_string):

    dict_string = dict_string.replace("'", '"').replace('u"', '"')
    return json.loads(dict_string)



def get_available_dates(file_name):
    snapshots=pd.read_csv(file_name)
    snapshots['snapshots']=snapshots['snapshots'].apply(string_to_dict)
    return list(snapshots.iloc[0,2].keys())

def from_iso_to_date(iso_date):
    return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").date()

def create_solana_tvl_table(execution_id,dune_key):
    solana=get_query_result(execution_id,dune_key)
    solana['date']=solana['d'].apply(from_iso_to_date)
    solana['tvl']=solana['stsol_supply']*solana['price']
    solana['network']='solana'
    result_df=solana[['date','tvl','network']]
    return result_df


def create_kusama_tvl_table(file_name):
    kusama=pd.read_csv(file_name)
    kusama=kusama.rename(columns={'Time':'timestamp','{instance="ksm-monitor.prod.lido.mixbytes.io:80", job="monitor"}':'amount'})
    kusama['date']=pd.to_datetime(kusama['timestamp'])
    kusama['date']=kusama['date'].dt.date
    kusama=kusama.groupby('date').tail(1)
    prices=requests.get('https://api.coingecko.com/api/v3/coins/kusama/market_chart/range?vs_currency=usd&from=1656536400&to=1662411600')
    prices_raw=prices.json()['prices']
    kusama_price=pd.DataFrame(prices_raw,columns=['timestamp','price'])
    kusama_price['date']=pd.to_datetime(kusama_price['timestamp'],unit='ms')
    kusama_price['date']=kusama_price['date'].dt.date
    kusama_price=kusama_price.groupby('date').tail(1)
    kusama=kusama.merge(kusama_price,on='date',how='inner')
    kusama['tvl']=kusama['amount']*kusama['price']
    kusama['network']='kusama'
    kusama_result=kusama[['date','tvl','network']]
    return kusama_result

def create_polkadot_tvl_table(file_name):
    polkadot=pd.read_csv(file_name)
    polkadot=polkadot.rename(columns={'Time':'timestamp','{instance="dot-monitor.prod.lido.mixbytes.io:80", job="monitor"}':'amount'})
    polkadot['date']=pd.to_datetime(polkadot['timestamp'])
    polkadot['date']=polkadot['date'].dt.date
    polkadot=polkadot.groupby('date').tail(1)
    prices=requests.get('https://api.coingecko.com/api/v3/coins/polkadot/market_chart/range?vs_currency=usd&from=1656536400&to=1662411600')
    prices_raw=prices.json()['prices']
    polkadot_price=pd.DataFrame(prices_raw,columns=['timestamp','price'])
    polkadot_price['date']=pd.to_datetime(polkadot_price['timestamp'],unit='ms')
    polkadot_price['date']=polkadot_price['date'].dt.date
    polkadot_price=polkadot_price.groupby('date').tail(1)
    polkadot=polkadot.merge(polkadot_price,on='date',how='inner')
    polkadot['tvl']=polkadot['amount']*polkadot['price']
    polkadot['network']='polkadot'
    kusama_result=polkadot[['date','tvl','network']]
    return kusama_result

def create_polygon_tvl_table(execution_id, dune_key):
    matic=get_query_result(execution_id,dune_key)
    matic=matic.rename(columns={'TVL($)':'tvl','time':'time'})
    matic['date']=pd.to_datetime(matic['time']).dt.date
    matic['network']='polygon'
    matic_result=matic[['date','tvl','network']]
    return matic_result

def create_ethereum_tvl_table(execution_id, dune_key):
    ethereum=get_query_result(execution_id,dune_key)
    ethereum=ethereum.rename(columns={'TVL':'tvl'})
    ethereum['date']=pd.to_datetime(ethereum['day']).dt.date
    ethereum['network']='ethereum'
    ethereum_result=ethereum[['date','tvl','network']]
    return ethereum_result

def create_total_tvl(dune_key):
    ethereum_data=create_ethereum_tvl_table('01GCFHE8139Z9TPPHKCGB5BYPC',dune_key)
    matic_data=create_polygon_tvl_table('01GCFHDPH91BXN38YFDN0DFFE2',dune_key)
    solana_data=create_solana_tvl_table('01GCFH2BE4PKGFMMZY08H1Z75V',dune_key)
    kusama_data=create_kusama_tvl_table('Total_stacked_stKSM.csv')
    polkadot_data=create_polkadot_tvl_table('Total_stacked_stDOT.csv')
    pdList=[ethereum_data,matic_data,solana_data,kusama_data,polkadot_data]
    total_tvl=pd.concat(pdList)
    return total_tvl

def create_kusama_depositors_table(file_name):
    ksm_holders=pd.read_csv(file_name)
    ksm_holders=ksm_holders.rename(columns={'Time':'timestamp','monitor_holders_amount{instance="ksm-monitor.prod.lido.mixbytes.io:80", job="monitor"}':'ksm_depositors'})
    ksm_holders['date']=pd.to_datetime(ksm_holders['timestamp']).dt.date
    ksm_holders=ksm_holders.groupby('date').tail(1)
    ksm_holders_result=ksm_holders[['date','ksm_depositors']]
    return ksm_holders_result


def create_polkadot_depositors_table(file_name):
    dot_holders=pd.read_csv(file_name)
    dot_holders=dot_holders.rename(columns={'Time':'timestamp','monitor_holders_amount{instance="dot-monitor.prod.lido.mixbytes.io:80", job="monitor"}':'dot_depositors'})
    dot_holders['date']=pd.to_datetime(dot_holders['timestamp']).dt.date
    dot_holders=dot_holders.groupby('date').tail(1)
    dot_holders_result=dot_holders[['date','dot_depositors']]
    return dot_holders_result


def create_solana_depositors_table(execution_id,dune_key):
    solana=get_query_result(execution_id,dune_key)
    solana=solana.rename(columns={'cum_users':'sol_depositors'})
    solana['date']=solana['d'].apply(from_iso_to_date)
    solana_depositors=solana[['date','sol_depositors']]
    return solana_depositors

def create_matic_depositors_table(execution_id,dune_key):
    matic_eth_depositors=get_query_result(execution_id,dune_key)
    matic_eth_depositors['date']=pd.to_datetime(matic_eth_depositors['time']).dt.date
    matic_eth_depositors=matic_eth_depositors.groupby('date').tail(1)
    matic_eth_depositors_final=matic_eth_depositors[['ETH_depositors','MATIC_depositors','date']]
    return matic_eth_depositors_final

def create_total_depositor_table(dune_key):
    matic_eth_depositors=create_matic_depositors_table('01GCFHDPH91BXN38YFDN0DFFE2',dune_key)
    sol_depositors=create_solana_depositors_table('01GCFH306YNAXXAT1VHV867R2P',dune_key)
    kusama_depositors=create_kusama_depositors_table('Holders_amount_stKSM.csv')
    dot_depositors=create_polkadot_depositors_table('Holders_amount_stDOT.csv')
    total=matic_eth_depositors.merge(sol_depositors,on='date',how='left')
    total=total.merge(kusama_depositors,on='date',how='left')
    total=total.merge(dot_depositors,on='date',how='left')
    total=total.fillna(0)
    return total