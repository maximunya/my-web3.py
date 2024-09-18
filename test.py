from web3 import Web3

w3 = Web3(Web3.HTTPProvider([
    'https://bsc-dataseed.binance.org/',
    'https://bsc-dataseed1.defibit.io/',
    'https://bsc-dataseed1.ninicoin.io/',
]))
print(w3.is_connected())
print(w3.is_connected())
print(w3.is_connected())

