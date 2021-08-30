"""
Text Embedder.
Author: Daan Kooij
Last modified: August 5th, 2021
"""

import itertools
import math
import torch
from transformers import AutoTokenizer, AutoModelForMaskedLM

import csv_reader
import detect_html


OUTPUT_DIR = "tensor"


def initialize_tokenizer():
    return AutoTokenizer.from_pretrained("pdelobelle/robbert-v2-dutch-base")


def initialize_model():
    # Load model
    model = AutoModelForMaskedLM.from_pretrained("pdelobelle/robbert-v2-dutch-base", output_hidden_states=True)
    model.eval()

    # Move model to GPU if available
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = model.to(device)

    # Return model
    return model


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


def convert_to_tensor(list_of_token_lists):
    # token_list_of_lists = [[p1t1, p1t2, ...], [p2t1, p2t2, ...], [p3t1], [p4t1, p4t2, ...], ...]
    return torch.cat([torch.LongTensor(token_list) for token_list in list_of_token_lists])


def get_embeddings(model, tensor):
    with torch.no_grad():
        out = model(input_ids=tensor)

    final_hidden_layer = out.hidden_states[-1]
    embeddings = torch.mean(final_hidden_layer, dim=1)

    return embeddings


def get_mean_embedding(embeddings):
    return torch.mean(embeddings, dim=0)


def get_page_text(page_html):
    words = []

    for p in page_html.find_all("p"):
        line_words = p.get_text().strip().split()
        words.extend(line_words)

    return " ".join(words)


def store_tensor(tensor, log_entry):
    output_path = OUTPUT_DIR + "/" + csv_reader.get_filename(log_entry) + ".pt"
    torch.save(tensor, output_path)


def crawl_to_embeddings(start_index=0):
    tokenizer = initialize_tokenizer()
    padding_token = dict(zip(tokenizer.all_special_tokens, tokenizer.all_special_ids))["<pad>"]
    model = initialize_model()

    for i, log_entry in zip(itertools.count(), csv_reader.get_all_log_entries()):
        if start_index > i:
            continue
        with open(csv_reader.get_filepath(log_entry)) as file:
            page_html = detect_html.get_html(file)
            if page_html:  # If the HTML can be parsed successfully
                page_text = get_page_text(page_html)
                token_lists = encode_text(tokenizer, padding_token, page_text)
                tensor = convert_to_tensor([token_lists])
                # TODO: If page_text is long, there will be a lot of token lists.
                # TODO: This can result in excessive memory usage when calling get_embeddings.
                # TODO: Investigate: is the problem still occurring when using GPU?
                # TODO: Alternative solution: use smaller batches.
                text_embeddings = get_embeddings(model, tensor)
                mean_embedding = get_mean_embedding(text_embeddings)
                store_tensor(mean_embedding, log_entry)


crawl_to_embeddings()
