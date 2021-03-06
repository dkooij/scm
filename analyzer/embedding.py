"""
Text Embedder.
Author: Daan Kooij
Last modified: September 15th, 2021
"""

import itertools
import math
import torch
from transformers import AutoTokenizer, AutoModelForMaskedLM

import csv_reader
import detect_html


BATCH_LIMIT = 16


def initialize_tokenizer():
    return AutoTokenizer.from_pretrained("pdelobelle/robbert-v2-dutch-base")


def get_padding_token(tokenizer):
    return dict(zip(tokenizer.all_special_tokens, tokenizer.all_special_ids))["<pad>"]


def initialize_model():
    # Load model
    model = AutoModelForMaskedLM.from_pretrained("pdelobelle/robbert-v2-dutch-base", output_hidden_states=True)
    model.eval()

    # Move model to GPU if available
    device = get_compute_device()
    model = model.to(device)

    # Return model
    return model, device


def get_compute_device():
    return "cuda" if torch.cuda.is_available() else "cpu"


def get_model_quad():
    tokenizer = initialize_tokenizer()
    padding_token = get_padding_token(tokenizer)
    model, device = initialize_model()

    return tokenizer, padding_token, model, device


def encode_text(tokenizer, padding_token, text, max_size=512):
    text_ids = tokenizer.encode(text)
    token_vectors = []

    num_parts = math.ceil(len(text_ids) / max_size)
    avg_size = len(text_ids) / num_parts

    for i in range(num_parts):
        start_index, end_index = math.ceil(i * avg_size), math.ceil((i + 1) * avg_size)
        token_vector = text_ids[start_index:end_index]
        padding_required = max_size - end_index + start_index
        token_vector.extend([padding_token for _ in range(padding_required)])
        token_vectors.append(token_vector)

    return token_vectors


def convert_to_tensor(list_of_token_lists, device):
    # token_list_of_lists = [[p1t1, p1t2, ...], [p2t1, p2t2, ...], [p3t1], [p4t1, p4t2, ...], ...]
    tensor = torch.cat([torch.LongTensor(token_list) for token_list in list_of_token_lists])
    return tensor.to(device)


def get_embeddings(model, tensor_input):
    tensor_batches = torch.split(tensor_input, BATCH_LIMIT)
    embeddings_batches = []

    for tensor_batch in tensor_batches:
        with torch.no_grad():
            out = model(input_ids=tensor_batch)
        final_hidden_layer = out.hidden_states[-1]
        embeddings_batch = torch.mean(final_hidden_layer, dim=1)
        embeddings_batches.append(embeddings_batch)

    embeddings_output = torch.cat(embeddings_batches)

    return embeddings_output


def get_mean_embedding(embeddings):
    return torch.mean(embeddings, dim=0)


def store_tensor(tensor, log_entry, output_dir):
    output_path = output_dir + "/" + csv_reader.get_filename(log_entry) + ".pt"
    torch.save(tensor, output_path)


def compute_embedding(log_entry, model_quad, output_dir, page_text=None):
    if page_text is None:
        page_text = log_entry["Page text"]

    tokenizer, padding_token, model, device = model_quad

    token_lists = encode_text(tokenizer, padding_token, page_text)
    tensor = convert_to_tensor([token_lists], device)
    text_embeddings = get_embeddings(model, tensor)
    mean_embedding = get_mean_embedding(text_embeddings)
    store_tensor(mean_embedding, log_entry, output_dir)


def crawl_to_embeddings(input_dir, output_dir, start_index=0):
    model_quad = get_model_quad()

    for i, log_entry in zip(itertools.count(), csv_reader.get_all_log_entries(input_dir)):
        if start_index > i:
            continue
        with open(csv_reader.get_filepath(log_entry, input_dir), "rb") as file:
            page_html = detect_html.get_html(file)
            if page_html:  # If the HTML can be parsed successfully
                page_text = detect_html.get_page_text(page_html)
                compute_embedding(log_entry, model_quad, output_dir, page_text=page_text)
