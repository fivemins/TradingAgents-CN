import json
from urllib import error, parse, request

import chromadb
from chromadb.config import Settings
from openai import OpenAI

from tradingagents.provider_utils import (
    get_embedding_api_key,
    get_embedding_base_url,
    is_local_base_url,
)


class FinancialSituationMemory:
    def __init__(self, name, config):
        self.enabled = bool(config.get("enable_memory", True))
        self.embedding = config.get("embedding_model", "")
        self.client = None
        self.chroma_client = None
        self.situation_collection = None
        self.embedding_base_url = get_embedding_base_url(config) or ""
        self.embedding_api_key = get_embedding_api_key(config)

        if (
            not self.enabled
            or not self.embedding_base_url
            or not self.embedding_api_key
            or not self.embedding
        ):
            self.enabled = False
            return

        self.client = OpenAI(
            base_url=self.embedding_base_url,
            api_key=self.embedding_api_key,
        )
        self.chroma_client = chromadb.Client(Settings(allow_reset=True))
        self.situation_collection = self.chroma_client.get_or_create_collection(name=name)

    def _embedding_headers(self):
        headers = {"Content-Type": "application/json"}
        use_auth = bool(self.embedding_api_key)
        if is_local_base_url(self.embedding_base_url):
            use_auth = (
                "11434" not in self.embedding_base_url
                and self.embedding_api_key
                not in {"local-placeholder-key", "ollama-local"}
            )
        if use_auth:
            headers["Authorization"] = f"Bearer {self.embedding_api_key}"
        return headers

    def _post_json(self, url, payload):
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            headers=self._embedding_headers(),
            method="POST",
        )
        if is_local_base_url(self.embedding_base_url):
            opener = request.build_opener(request.ProxyHandler({}))
            response_context = opener.open(req, timeout=30)
        else:
            response_context = request.urlopen(req, timeout=30)
        with response_context as response:
            return json.loads(response.read().decode("utf-8"))

    def _local_embedding_urls(self):
        normalized = self.embedding_base_url.rstrip("/")
        urls = [f"{normalized}/embeddings"]

        parsed = parse.urlparse(normalized)
        root = normalized
        if parsed.path.endswith("/v1"):
            root = normalized[: -len("/v1")]

        urls.extend(
            [
                f"{root}/api/embed",
                f"{root}/api/embeddings",
            ]
        )
        return urls

    def _extract_embedding(self, response_json):
        data = response_json.get("data")
        if data and isinstance(data, list):
            first = data[0] or {}
            embedding = first.get("embedding")
            if embedding:
                return embedding

        embedding = response_json.get("embedding")
        if embedding:
            return embedding

        embeddings = response_json.get("embeddings")
        if embeddings and isinstance(embeddings, list):
            first = embeddings[0]
            if isinstance(first, list):
                return first

        raise ValueError("Embedding response did not contain vectors.")

    def _get_local_embedding(self, text):
        last_error = None
        payloads = {
            "embeddings": {"model": self.embedding, "input": text},
            "embed": {"model": self.embedding, "input": text},
            "api_embeddings": {"model": self.embedding, "prompt": text},
        }

        for url in self._local_embedding_urls():
            try:
                if url.endswith("/api/embeddings"):
                    response_json = self._post_json(url, payloads["api_embeddings"])
                elif url.endswith("/api/embed"):
                    response_json = self._post_json(url, payloads["embed"])
                else:
                    response_json = self._post_json(url, payloads["embeddings"])
                return self._extract_embedding(response_json)
            except (error.HTTPError, error.URLError, ValueError, KeyError) as exc:
                last_error = exc

        if last_error is not None:
            raise RuntimeError(
                f"Unable to fetch local embedding from {self.embedding_base_url}: "
                f"{last_error}"
            ) from last_error

        raise RuntimeError("Unable to fetch local embedding.")

    def get_embedding(self, text):
        """Get OpenAI embedding for a text"""
        if not self.enabled or self.client is None:
            raise RuntimeError("FinancialSituationMemory is disabled.")

        if is_local_base_url(self.embedding_base_url):
            return self._get_local_embedding(text)

        response = self.client.embeddings.create(model=self.embedding, input=text)
        return response.data[0].embedding

    def add_situations(self, situations_and_advice):
        """Add financial situations and their corresponding advice. Parameter is a list of tuples (situation, rec)"""
        if not self.enabled or not situations_and_advice:
            return

        situations = []
        advice = []
        ids = []
        embeddings = []

        offset = self.situation_collection.count()

        for i, (situation, recommendation) in enumerate(situations_and_advice):
            situations.append(situation)
            advice.append(recommendation)
            ids.append(str(offset + i))
            embeddings.append(self.get_embedding(situation))

        self.situation_collection.add(
            documents=situations,
            metadatas=[{"recommendation": rec} for rec in advice],
            embeddings=embeddings,
            ids=ids,
        )

    def get_memories(self, current_situation, n_matches=1):
        """Find matching recommendations using OpenAI embeddings"""
        if (
            not self.enabled
            or self.situation_collection is None
            or self.situation_collection.count() == 0
        ):
            return []

        query_embedding = self.get_embedding(current_situation)

        results = self.situation_collection.query(
            query_embeddings=[query_embedding],
            n_results=n_matches,
            include=["metadatas", "documents", "distances"],
        )

        matched_results = []
        for i in range(len(results["documents"][0])):
            matched_results.append(
                {
                    "matched_situation": results["documents"][0][i],
                    "recommendation": results["metadatas"][0][i]["recommendation"],
                    "similarity_score": 1 - results["distances"][0][i],
                }
            )

        return matched_results


if __name__ == "__main__":
    # Example usage
    matcher = FinancialSituationMemory()

    # Example data
    example_data = [
        (
            "High inflation rate with rising interest rates and declining consumer spending",
            "Consider defensive sectors like consumer staples and utilities. Review fixed-income portfolio duration.",
        ),
        (
            "Tech sector showing high volatility with increasing institutional selling pressure",
            "Reduce exposure to high-growth tech stocks. Look for value opportunities in established tech companies with strong cash flows.",
        ),
        (
            "Strong dollar affecting emerging markets with increasing forex volatility",
            "Hedge currency exposure in international positions. Consider reducing allocation to emerging market debt.",
        ),
        (
            "Market showing signs of sector rotation with rising yields",
            "Rebalance portfolio to maintain target allocations. Consider increasing exposure to sectors benefiting from higher rates.",
        ),
    ]

    # Add the example situations and recommendations
    matcher.add_situations(example_data)

    # Example query
    current_situation = """
    Market showing increased volatility in tech sector, with institutional investors 
    reducing positions and rising interest rates affecting growth stock valuations
    """

    try:
        recommendations = matcher.get_memories(current_situation, n_matches=2)

        for i, rec in enumerate(recommendations, 1):
            print(f"\nMatch {i}:")
            print(f"Similarity Score: {rec['similarity_score']:.2f}")
            print(f"Matched Situation: {rec['matched_situation']}")
            print(f"Recommendation: {rec['recommendation']}")

    except Exception as e:
        print(f"Error during recommendation: {str(e)}")
