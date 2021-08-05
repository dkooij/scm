"""
Text Embedder.
Author: Daan Kooij
Last modified: August 4th, 2021
"""

import math
import torch
from transformers import AutoTokenizer, AutoModelForMaskedLM


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


def convert_to_tensor(token_list_of_lists):
    # token_list_of_lists = [[p1t1, p1t2, ...], [p2t1, p2t2, ...], [p3t1], [p4t1, p4t2, ...], ...]
    return torch.cat([torch.LongTensor(token_list) for token_list in token_list_of_lists])


def get_embeddings(model, tensor):
    with torch.no_grad():
        out = model(input_ids=tensor)

    final_hidden_layer = out.hidden_states[-1]
    sentence_embeddings = torch.mean(final_hidden_layer, dim=1)

    return sentence_embeddings


def main():
    tokenizer = initialize_tokenizer()
    padding_token = dict(zip(tokenizer.all_special_tokens, tokenizer.all_special_ids))["<pad>"]
    model = initialize_model()

    texts = ["Goedemorgen! Het is de start van een nieuwe, mooie dag.",
             "Goedemorgen! Het is de start van een nieuwe, fijne dag.",
             "Goedemorgen! Het is de start van een nieuwe, slechte dag.",
             "Hallo! Hoe gaat het met jullie vandaag?",
             "De Tennishal Sneek is een sporthal in de stad Sneek die gebruikt wordt voor de tennissport.",
             "De Tennishal Sneek is een sporthal in de stad Sneek die gebruikt wordt voor de tennissport.",
             "De Tennishal Sneek is een sporthal in het dorp Sneek die gebruikt wordt voor de tennissport.",
             "De studieruimte Sneek is een sporthal in het dorp Sneek die gebruikt wordt voor de tennissport."
             ]

    token_list_of_lists = [encode_text(tokenizer, padding_token, text) for text in texts]
    tensor = convert_to_tensor(token_list_of_lists)
    embeddings = get_embeddings(model, tensor)

    for (i1, l1) in zip(range(100), embeddings):
        l1 = l1.tolist()
        for (i2, l2) in zip(range(100), embeddings):
            l2 = l2.tolist()
            x = sum([abs(sum(v)) for v in zip(l1, [-pos for pos in l2])]) / len(l1)
            print(i1, i2, "{:.6f}".format(x))
        print()

    print("Done")


main()
