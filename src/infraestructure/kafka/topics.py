"""Definição dos topicos Kafka utilizados na aplicação.

Centralizados aqui para evitar strings mágicas espalhadas pelo código e facilitar a manutenção.
"""

TOPIC_RAW_MARKET = "raw.market"
TOPIC_RAW_NEWS = "raw.news"
TOPIC_RAW_TWEETS = "raw.tweets"
TOPIC_RAW_CDS = "raw.cds"
TOPIC_RAW_FEEDBACK = "raw.feedback"
TOPIC_EVENTS_ISMB = "events.ismb"

ALL_TOPICS = [
    TOPIC_RAW_MARKET,
    TOPIC_RAW_NEWS,
    TOPIC_RAW_TWEETS,
    TOPIC_RAW_CDS,
    TOPIC_RAW_FEEDBACK,
    TOPIC_EVENTS_ISMB,
]
