from src.config import load_config
from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from src.api.gamma import GammaClient
from src.strategy.scanner import MarketScanner
from src.strategy.scorer import CandidateScorer

config = load_config()
pk = config.wallet.private_key

l1 = PyClobClient(host='https://clob.polymarket.com', chain_id=137, key=pk)
creds = l1.create_or_derive_api_creds()
l2 = PyClobClient(host='https://clob.polymarket.com', chain_id=137, key=pk, creds=creds)

gamma = GammaClient(config.api.gamma_base_url, config.api.page_size, config.api.request_delay_ms)
scanner = MarketScanner(gamma, config.strategy)
scorer = CandidateScorer(config.strategy)
ranked = scorer.rank(scanner.scan(), top_n=1)
sc = ranked[0]
print(f'Market: {sc.market.question[:50]} / {sc.outcome} @ {sc.price}')

bet_usd = 1.0
size = round(bet_usd / sc.price, 2)
print(f'Bet: ${bet_usd} | Size: {size} shares @ ${sc.price}')

args = OrderArgs(token_id=sc.token_id, price=sc.price, size=size, side='BUY')
order = l2.create_order(args)
print('Order signed OK')
resp = l2.post_order(order, orderType=OrderType.FOK)
print(f'Response: {resp}')
