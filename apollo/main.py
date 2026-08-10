# done: refactor the run() methods from play_store.py and marketaux.py so that when a collection
# holding the references of both children of BaseScraper (PlayStoreScraper and MarketauxScraper)
# both of them holding the same set of parameters and attributes should be able to run in one go
# by just calling an orchestrator 

# TODO: make the async orchestrator streaming the data right into kafka into their respective topics