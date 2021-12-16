"""
Extract miscellaneous static HTML features from a crawled page.
Author: Daan Kooij
Last modified: December 16th, 2021
"""


def get_page_html_features(page_html):
    # Retrieve images, scripts, tables, meta properties, and HTML tags
    images, scripts, tables, metas, tags = [], [], [], [], []
    for image in page_html.find_all("img"):
        images.append(str(image))
    for script in page_html.find_all("script"):
        scripts.append(str(script))
    for table in page_html.find_all("table"):
        tables.append(str(table))
    for meta in page_html.find_all("meta"):
        metas.append(str(meta))
    for tag in page_html.find_all():
        tags.append(tag.name)
    return images, scripts, tables, metas, tags
