import logging
import os
import re
from functools import cache
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Union

import yaml
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

PROJECT_ROOT = (Path(__file__).parent / ".." / "..").resolve()
DEFAULT_CONFIG_LOCATION = PROJECT_ROOT / "es_config.yml"


@cache
def es_init(config: Path = DEFAULT_CONFIG_LOCATION, timeout: int = 30) -> Elasticsearch:
    """
    :param config: Path to the config yaml file, containing `cloud_id` and `api_key` fields.
    :return: Authenticated ElasticSearch client.
    """
    with open(config) as file_ref:
        config = yaml.safe_load(file_ref)
    
    if config == DEFAULT_CONFIG_LOCATION:
        logger.warning("Using default config file. This will unlikely work unless you're the creator of this library. Please make sure to specify the ES config file and provide it to the function you are using.")

    cloud_id = config["cloud_id"]
    api_key = config.get("api_key", os.getenv("ES_API_KEY", None))
    if not api_key:
        raise RuntimeError(
            f"Please specify ES_API_KEY environment variable or add api_key to {DEFAULT_CONFIG_LOCATION}."
        )

    es = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key,
        retry_on_timeout=True,
        http_compress=True,
        request_timeout=timeout,
    )

    return es


def get_indices(
    return_mapping: bool = False, es: Optional[Elasticsearch] = None
) -> Dict:
    """
    :param return_mapping: Whether to return mapping along with index information.
    :return: Dictionary of existing indices.

    Note that this function won't work in case you were given an API key from AI2 since these are restrictive.
    Please refer to the README for the indices mapping.
    """
    es = es or es_init()

    indices = es.cat.indices(format="json")
    exclude = [
        "search-test",
        "test-index-2",
        "metrics-endpoint.metadata_current_default",
    ]
    indices = {
        index["index"]: {key: index[key] for key in ["docs.count"]}
        for index in indices
        if not index["index"].startswith(".") and not index["index"] in exclude
    }

    if return_mapping:
        mappings = es.indices.get_mapping(index=list(indices.keys()))
        for key in mappings:
            indices[key]["properties"] = list(
                mappings[key]["mappings"]["properties"].keys()
            )

    return indices


def _query_documents_contain_phrases(
    phrases: Union[str, List[str]],
    all_phrases: bool = False,
    do_score: bool = False,
    is_regexp: bool = False,
    slop: int = 0,
) -> Dict:
    if isinstance(phrases, str):
        phrases = [phrases]
    if all_phrases:
        which_bool = "must" if do_score else "filter"
        minimum_should_match = None
    else:
        which_bool = "should"
        minimum_should_match = 1

    if is_regexp:
        match_query = []
        for phrase in phrases:
            match_query.append(
                {
                    "regexp": {
                        "text": {
                            "value": phrase,
                            "case_insensitive": True,
                            "flags": "ALL",
                        }
                    }
                }
            )
        # minimum_should_match = None
    else:
        match_query = []
        for phrase in phrases:
            match_query.append({"match_phrase": {"text": {"query": phrase, "slop": slop}}})

    query = {
        "bool": {which_bool: match_query, "minimum_should_match": minimum_should_match}
    }
    return query


def count_documents_containing_phrases(
    index: str,
    phrases: Union[str, List[str]],
    all_phrases: bool = False,
    is_regexp: bool = False,
    slop: int = 0,
    subset_filter: Optional[List[Dict[str, Any]]] = None,
    es: Optional[Elasticsearch] = None,
) -> int:
    """
    :param index: Name of the index
    :param phrases: A single string or a list of strings to be matched in the `text` field
        of the index.
    :param all_phrases: Whether the document should contain all phrases (AND clause) or any
        of the phrases (OR clause).
    :param is_regexp: Whether the phrases are regular expressions. Note that spaces in regular
        expressions are not supported by ElasticSearch, so if you want to do an exact match for
        spans longer than a single term, set this to False. In most cases, using exp1|exp2 is better
        than specifying [exp1, exp2] as two different `phrases`.
    :param slop: The number of positions allowed between matching phrases. Each phrase is allowed to 
        match a string where there are `slop` positions between the terms.
        For instance, "I like" qeury with a slop=1 will match both strings that literally contain "I like"
        but also "I really like" and "I would like", etc.
    :return: The number of documents matching the conditions.

    Examples:

        count_documents_containing_phrases("test-index", "legal")  # single term
        count_documents_containing_phrases("test-index", ["legal", "license"])  # list of terms
        count_documents_containing_phrases("test-index", ["terms of use", "legally binding"])  # list of word sequences

        # The documents should contain both `winter` and `spring` in the text.
        count_documents_containing_phrases("test-index", ["winter", "spring"], all_phrases=True)

    """
    es = es or es_init()

    query = _query_documents_contain_phrases(phrases, all_phrases, is_regexp=is_regexp, slop=slop)

    if index == "c4":
        if not subset_filter:
            subset_filter = [{"subset": "en"}]
        else:
            subset_filter.append({"subset": "en"})

    if subset_filter:
        if "filter" not in query["bool"]:
            query["bool"]["filter"] = []
        for filter_ in subset_filter:
            query["bool"]["filter"].append({"term": filter_})

    result = es.count(index=index, query=query)

    return result["count"]


def get_documents_containing_phrases(
    index: str,
    phrases: Union[str, List[str]],
    all_phrases: bool = False,
    num_documents: int = 10,
    is_regexp: bool = False,
    slop: int = 0,
    return_all_hits: bool = False,
    sort_field: str = "date",
    subset_filter: Optional[Dict[str, Any]] = None,
    es: Optional[Elasticsearch] = None,
) -> Generator[Dict, None, None]:
    """
    :param index: Name of the index
    :param phrases: A single string or a list of strings to be matched in the `text` field
        of the index.
    :param all_phrases: Whether the document should contain all phrases (AND clause) or any
        of the phrases (OR clause).
    :param num_documents: The number of document hits to return.
    :param is_regexp: Whether the phrases are regular expressions. Note that spaces in regular
        expressions are not supported by ElasticSearch, so if you want to do an exact match for
        spans longer than a single term, set this to False. In most cases, using exp1|exp2 is better
        than specifying [exp1, exp2] as two different `phrases`.
    :param return_all_hits: Whether to return all hits beyond maximum 10k results. This will return an
        iterator.
    :return: An iterable (of length `num_documents` if `return_all_hits` is False),
        containing the relevant hits.

    Examples:

        get_documents_containing_phrases("test-index", "legal", num_documents=50)  # single term, get 50 documents
        get_documents_containing_phrases("test-index", ["legal", "license"])  # list of terms
        get_document_containing_phrases("test-index", ["terms of use", "legally binding"])  # list of word sequences

        # The documents should contain both `winter` and `spring` in the text.
        get_documents_containing_phrases("test-index", ["winter", "spring"], all_phrases=True)
    """
    es = es or es_init()

    query = _query_documents_contain_phrases(phrases, all_phrases, is_regexp=is_regexp, slop=slop)
    if index == "c4":
        if not subset_filter:
            subset_filter = [{"subset": "en"}]
        else:
            subset_filter.append({"subset": "en"})

    if subset_filter:
        if "filter" not in query["bool"]:
            query["bool"]["filter"] = []
        for filter_ in subset_filter:
            query["bool"]["filter"].append({"term": filter_})

    if return_all_hits:
        sort = [{sort_field: "asc"}]
        pit = es.open_point_in_time(index=index, keep_alive="1m")
        # pit_search = {"id": pit["id"], "keep_alive": "1m"}
        # all_results = []
        results = es.search(index=index, query=query, size=num_documents, sort=sort)[
            "hits"
        ]["hits"]
        yield from results
        while len(results) > 0:
            # todo: perhaps we need to refresh pit?
            results = es.search(
                index=index,
                query=query,
                size=num_documents,
                sort=sort,
                search_after=results[-1]["sort"],
            )["hits"]["hits"]
            yield from results
        try:
            es.close_point_in_time(id=pit["id"])
        except NotFoundError:
            # Already closed.
            pass
    else:
        yield from es.search(index=index, query=query, size=num_documents)["hits"][
            "hits"
        ]


def count_documents_for_each_phrase(
    index: str,
    phrases: Union[str, Iterable[str], Iterable[List[str]]],
    batch_size: int = 500,
    timeout: str = "60s",
    all_phrases: bool = False,
    subset_filter: Optional[Dict[str, Any]] = None,
    es: Optional[Elasticsearch] = None,
):
    if isinstance(phrases, str):
        phrases = [phrases]

    if all_phrases:
        try:
            assert isinstance(phrases, Iterable)
            assert isinstance(phrases[0], List)
            assert isinstance(phrases[0][0], str)
        except AssertionError:
            raise AssertionError(
                "`all_phrases` is set to True, please provide lists of lists."
            )
    else:
        try:
            assert isinstance(phrases, Iterable)
            assert isinstance(phrases[0], str)
        except AssertionError:
            raise AssertionError(
                "`all_phrases` is set to False, please provide a list of strings."
            )
    es = es or es_init()
    # num_shards = len(es.cat.shards(index=index, format="json"))
    final_counts = []

    if index == "c4":
        if not subset_filter:
            subset_filter = [{"subset": "en"}]
        else:
            subset_filter.append({"subset": "en"})

    done = False
    generator = iter(phrases)
    while not done:
        queries = []
        for i, phrase in enumerate(generator):
            if not isinstance(phrase, List):
                phrase = [phrase]
            match_query = []
            for phr in phrase:
                match_query.append({"match_phrase": {"text": phr}})

            if subset_filter:
                for filter_ in subset_filter:
                    match_query.append({"term": filter_})

            queries.append({"index": index, "search_type": "query_then_fetch"})
            queries.append(
                {
                    "stored_fields": [],
                    "timeout": timeout,
                    "track_scores": False,
                    "track_total_hits": True,
                    "query": {"bool": {"filter": match_query}},
                }
            )
            if i == batch_size:
                break
        if len(queries) == 0:
            done = True
            break
        results = es.msearch(
            index=index,
            searches=queries,
            search_type="query_then_fetch",
            rest_total_hits_as_int=True,
        )
        final_counts += [r["hits"]["total"] for r in results["responses"]]
    return final_counts


def count_total_occurrences_of_unigrams(
    index: str,
    unigrams: Union[str, List[str]],
    subset_filter: Optional[Dict[str, Any]] = None,
    es: Optional[Elasticsearch] = None,
) -> Dict[str, int]:
    """
    :param index: Name of the index
    :param terms: A single unigram or a list of unigrams to be matched in the `text` field
        of the index.
    :return: The total number of occurrences of each unigram in `terms` across all documents.

    Examples:

        count_total_occurrences_of_unigrams("test-index", "legal")  # single term
        count_total_occurrences_of_unigrams("test-index", ["legal", "license"])  # list of terms

    """
    if isinstance(unigrams, str):
        unigrams = [unigrams]

    es = es or es_init()

    # We use individual shards for counting total occurrences, because elasticsearch's default behavior
    # is to return term statistics for a randomly selected shard. For more information on term vector behaviour, please
    # see the following:
    # https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-termvectors.html#docs-termvectors-api-behavior
    num_shards = len(es.cat.shards(index=index, format="json"))

    logger.debug(f"Total number of shards in '{index}': {num_shards}")

    if index == "c4":
        if not subset_filter:
            subset_filter = [{"subset": "en"}]
        else:
            subset_filter.append({"subset": "en"})

    term_freq_dict = {}
    for term in unigrams:
        query = {"bool": {"filter": {"match": {"text": term}}}}
        if subset_filter:
            query = {"bool": {"filter": [{"match": {"text": term}}]}}
            for filter_ in subset_filter:
                query["bool"]["filter"].append(filter_)

        else:
            query = {"bool": {"filter": {"match": {"text": term}}}}

        total_freq = 0
        for i in range(num_shards):
            documents = es.search(
                index=index,
                query=query,
                preference=f"_shards:{i}",
                stored_fields=[],
                track_total_hits=False,
            )

            if len(documents["hits"]["hits"]) > 0:
                doc_id = documents["hits"]["hits"][0]["_id"]

                term_vector = es.termvectors(
                    index=index,
                    id=doc_id,
                    fields=["text"],
                    positions=False,
                    term_statistics=True,
                    preference=f"_shards:{i}",
                )

                ttf = term_vector["term_vectors"]["text"]["terms"][term]["ttf"]
                logger.debug(f"Total term frequency for shard {i}: {ttf}")
                total_freq += ttf

        logger.info(
            f"The term: '{term}' occurs {total_freq} times across all documents in '{index}'."
        )
        term_freq_dict[term] = total_freq
    return term_freq_dict
