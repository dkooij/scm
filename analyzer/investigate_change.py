"""
For a collection of crawled pages spanning two days,
investigate the type of changes the pages undergo.
Author: Daan Kooij
Last modified: November 4th, 2021
"""

import csv
from datetime import datetime
import itertools
from multiprocessing import Process
import os

import csv_reader
import detect_html
import extractor
from get_page_text import get_page_text


def get_timestamp():
    return datetime.now().strftime("%H:%M:%S") + " - "


def _extract_page_features_text(log_path, crawl_dir, output_dir, feature_names, tid_str):
    def output_filepath(name):
        return output_dir + "/" + crawl_dir.split("/")[-1] + "/" + name + "/" + name + "-" + tid_str + ".csv"

    def open_file(name):
        return open(output_filepath(name), "w", newline="", encoding="utf-8")

    def open_files(names):
        return [open_file(name) for name in names]

    def initialize_csv_writers(names_arg, files_arg):
        writers = []
        for n, f in zip(names_arg, files_arg):
            writer = csv.writer(f)
            writer.writerow(["Stage file", "URL index", n])
            writers.append(writer)
        return writers

    def write_row(writer, feature, log_entry_arg):
        writer.writerow([log_entry_arg["Stage file"], log_entry_arg["URL index"], feature])

    # Extracts both static simple page features and page text
    files = open_files(feature_names)
    try:
        csv_writers = initialize_csv_writers(feature_names, files)
        [text_writer, internal_outlinks_writer, external_outlinks_writer, email_links_writer,
         images_writer, scripts_writer, tables_writer, meta_writer, html_tags_writer] = csv_writers

        previous_page_text = None
        for log_entry in csv_reader.get_log_entries(log_path):
            with open(csv_reader.get_filepath(log_entry, crawl_dir), "rb") as file:
                page_text = get_page_text(file.read())

                if len(page_text) > 0 and page_text != previous_page_text:
                    file.seek(0)  # To allow reading the file again
                    page_html = detect_html.get_html(file)

                    if page_html:
                        # Write page text to CSV
                        write_row(text_writer, page_text, log_entry)

                        # Compute linkage features and write to CSV
                        internal_outlinks, external_outlinks, email_links = \
                            extractor.get_raw_linkage_features(log_entry, page_html)
                        write_row(internal_outlinks_writer, internal_outlinks, log_entry)
                        write_row(external_outlinks_writer, external_outlinks, log_entry)
                        write_row(email_links_writer, email_links, log_entry)

                        # Compute HTML features and write to CSV
                        images, scripts, tables, metas, html_tags = extractor.get_raw_html_features(page_html)
                        write_row(images_writer, images, log_entry)
                        write_row(scripts_writer, scripts, log_entry)
                        write_row(tables_writer, tables, log_entry)
                        write_row(meta_writer, metas, log_entry)
                        write_row(html_tags_writer, html_tags, log_entry)

                previous_page_text = page_text

    finally:
        for file in files:
            file.close()


def extract_page_features_text(crawls_root, input_dir, output_dir, feature_names):
    processes = []

    for name in feature_names:
        feature_output_dir = output_dir + "/" + input_dir + "/" + name
        os.makedirs(feature_output_dir, exist_ok=True)

    crawl_dir = crawls_root + "/" + input_dir
    for tid, log_path in zip(itertools.count(), csv_reader.get_log_paths(crawl_dir)):
        process = Process(target=_extract_page_features_text, args=(
            log_path, crawl_dir, output_dir, feature_names, str(tid)))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


"""
def extract_semantic_vectors(target, batch_index=0, start_index=0):
    import embedding  # Local import, because computationally expensive

    model_quad = embedding.get_model_quad()

    text_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
    tensor_dir = EXTRACT_ROOT + "/" + target + "/" + SV_DIR
    os.makedirs(tensor_dir, exist_ok=True)

    for i, log_path in zip(itertools.count(start=batch_index), csv_reader.get_log_paths(text_dir)):
        if i >= start_index:
            for log_entry in csv_reader.get_log_entries(log_path, ignore_validity_check=True):
                embedding.compute_embedding(log_entry, model_quad, tensor_dir)
            print(get_timestamp() + "Finished semantic embedding batch " + str(i))
"""


def combine_csv_files(input_dir, output_dir, names):
    """
    Combine collections of feature CSV files into
    one combined feature CSV file per name.
    """
    combined_output_dir = output_dir + "/" + input_dir + "/combined"
    os.makedirs(combined_output_dir, exist_ok=True)

    for name in names:
        entry_path = output_dir + "/" + input_dir + "/" + name

        entries = []
        for entry in csv_reader.get_all_log_entries(entry_path, ignore_validity_check=True):
            entries.append(entry)
        entries.sort(key=lambda e: (e["Stage file"], int(e["URL index"])))

        entry_output_path = combined_output_dir + "/" + name + ".csv"
        csv_reader.write_csv_file(entry_output_path, entries)


def compute_change(input1_dir, input2_dir, output_dir, names):
    differences_matrix = []

    for name in names:
        entry_path1 = output_dir + "/" + input1_dir + "/combined/" + name + ".csv"
        entry_path2 = output_dir + "/" + input2_dir + "/combined/" + name + ".csv"
        gen1 = csv_reader.get_log_entries(entry_path1, ignore_validity_check=True)
        gen2 = csv_reader.get_log_entries(entry_path2, ignore_validity_check=True)

        differences = []

        try:
            entry1, entry2 = next(gen1), next(gen2)
            while True:
                stage1, stage2 = int(entry1["Stage file"][-2:]), int(entry2["Stage file"][-2:])
                index1, index2 = int(entry1["URL index"]), int(entry2["URL index"])
                if stage1 == stage2:
                    if index1 == index2:

                        result = {"Stage file": entry1["Stage file"], "URL index": entry1["URL index"]}
                        for target_field in [name]:
                            result[target_field] = "0" if entry1[target_field] == entry2[target_field] else "1"
                        differences.append(result)

                    if index1 <= index2:
                        entry1 = next(gen1)
                    if index1 >= index2:
                        entry2 = next(gen2)
                elif stage1 < stage2:
                    entry1 = next(gen1)
                elif stage1 > stage2:
                    entry2 = next(gen2)
        except StopIteration:
            pass  # Done! Reached the end of one CSV file.

        differences_matrix.append(differences)

    combined_differences = []
    for i in range(len(differences_matrix[0])):  # Do for every difference list
        result = dict()
        for feature_dict in [differences[i] for differences in differences_matrix]:  # Do for every feature dictionary
            for k, v in feature_dict.items():
                if k in result:
                    assert v == result[k]
                else:
                    result[k] = v
        combined_differences.append(result)

    output_path = output_dir + "/differences.csv"
    csv_reader.write_csv_file(output_path, combined_differences)


def run():
    feature_names = ["text", "internal_outlinks", "external_outlinks", "email_links",
                     "images", "scripts", "tables", "meta", "html_tags"]

    # crawls_root = "C:/Users/daank/Drawer/SCM archives/Full crawls"
    # target_list = ["20210612", "20210613"]
    # output_dir = "output"
    crawls_root = "C:/Users/daank/Drawer/SCM archives/Crawl samples"
    target_list = ["testminiday", "testminiday2"]
    output_dir = "outputmini"

    # Iterate over the crawls of all days in target_list
    # for input_dir in target_list:
    #     extract_page_features_text(crawls_root, input_dir, output_dir, feature_names)
    #     combine_csv_files(input_dir, output_dir, feature_names)
    compute_change(target_list[0], target_list[1], output_dir, feature_names)


if __name__ == "__main__":
    run()
