import os

# Utils
import time
from typing import List

# Langchain
import langchain
from pydantic import BaseModel
from vertexai.language_models import TextGenerationModel

print(f"LangChain version: {langchain.__version__}")

# Vertex AI
from google.cloud import aiplatform
from langchain.chat_models import ChatVertexAI
from langchain.embeddings import VertexAIEmbeddings
from langchain.llms import VertexAI
from langchain.schema import HumanMessage, SystemMessage

from llm_experiments.utils import here

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(here() / 'motorway-genai-ccebd34bd403.json')


generation_model = TextGenerationModel.from_pretrained("text-bison@001")
prompt = "What is a large language model?"
response = generation_model.predict(prompt=prompt)
print(response.text)



# LLM model
llm = VertexAI(
    model_name="text-bison@001",
    max_output_tokens=256,
    temperature=0.1,
    top_p=0.8,
    top_k=40,
    verbose=True,
)

# class CustomVertexAIEmbeddings(VertexAIEmbeddings, BaseModel):
#     requests_per_minute: int
#     num_instances_per_batch: int
#
#     # Utility functions for Embeddings API with rate limiting
#     @staticmethod
#     def rate_limit(max_per_minute):
#         period = 60 / max_per_minute
#         print("Waiting")
#         while True:
#             before = time.time()
#             yield
#             after = time.time()
#             elapsed = after - before
#             sleep_time = max(0, period - elapsed)
#             if sleep_time > 0:
#                 print(".", end="")
#                 time.sleep(sleep_time)
#
#     # Overriding embed_documents method
#     def embed_documents(self, texts: List[str]):
#         limiter = self.rate_limit(self.requests_per_minute)
#         results = []
#         docs = list(texts)
#
#         while docs:
#             # Working in batches because the API accepts maximum 5
#             # documents per request to get embeddings
#             head, docs = (
#                 docs[: self.num_instances_per_batch],
#                 docs[self.num_instances_per_batch :],
#             )
#             chunk = self.client.get_embeddings(head)
#             results.extend(chunk)
#             next(limiter)
#
#         return [r.values for r in results]
#
# # Chat
# chat = ChatVertexAI()
#
# # Embedding
# EMBEDDING_QPM = 100
# EMBEDDING_NUM_BATCH = 5
# embeddings = CustomVertexAIEmbeddings(
#     requests_per_minute=EMBEDDING_QPM,
#     num_instances_per_batch=EMBEDDING_NUM_BATCH,
# )
#
#
# chat([HumanMessage(content="Hello")])
# res = chat(
#     [
#         SystemMessage(
#             content="You are a nice AI bot that helps a user figure out what to eat in one short sentence"
#         ),
#         HumanMessage(content="I like tomatoes, what should I eat?"),
#     ]
# )
#
# print(res.content)

