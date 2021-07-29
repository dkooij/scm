import torch
from transformers import AutoTokenizer, AutoModelForMaskedLM


tokenizer = AutoTokenizer.from_pretrained("pdelobelle/robbert-v2-dutch-base")
model = AutoModelForMaskedLM.from_pretrained("pdelobelle/robbert-v2-dutch-base", output_hidden_states=True)
model.eval()

text = "Goedemorgen! Het is de start van een nieuwe, mooie dag."
text_ids = tokenizer.encode(text)
text_ids_tensor = torch.LongTensor(text_ids)

# device = "cuda" if torch.cuda.is_available() else "cpu"
# model = model.to(device)

text_ids_tensor = text_ids_tensor.unsqueeze(0)

with torch.no_grad():
    out = model(input_ids=text_ids_tensor)

final_hidden_layer = out.hidden_states[-1]
sentence_embedding = torch.mean(final_hidden_layer[-1], dim=-2).squeeze()

print(sentence_embedding)
print("done")