"""
Extract the links from a crawled page.
Author: Daan Kooij
Last modified: November 30th, 2021
"""


# URL helper functions

def get_sl_domain(url):
    # Example: "https://a.b.c.nl/d/e/f" returns "c"
    try:
        return get_root_domain(url).rsplit(".", 2)[-2]
    except IndexError:
        return get_root_domain(url)


def get_root_domain(url):
    # Example: "https://a.b.c.nl/d/e/f" returns "a.b.c.nl"
    try:
        return strip_protocol(url).split("/", 1)[0]
    except IndexError:
        return strip_protocol(url)


def strip_protocol(url):
    while True:
        if url.startswith("https://"):
            url = url[8:]
        elif url.startswith("http://"):
            url = url[7:]
        else:
            return url


def href_to_url(href_url, root_domain, current_url):
    href_url = href_url.rstrip("/")  # Remove trailing "/"
    if len(href_url) == 0:
        return current_url
    elif href_url.startswith("http://") or href_url.startswith("https://"):
        return strip_protocol(href_url)
    elif href_url.startswith("/"):
        return root_domain + href_url
    else:
        return current_url + "/" + href_url


# Linkage feature extraction functions

def get_page_links(current_url, page_html):
    root_domain = get_root_domain(current_url)

    internal_outlinks, external_outlinks = [], []
    for a in page_html.find_all("a", href=True):
        href_url = a["href"].strip()
        if not href_url.startswith("mailto:"):
            link = href_to_url(href_url, root_domain, current_url)
            if get_sl_domain(link) == get_sl_domain(current_url):
                internal_outlinks.append(link)
            else:
                external_outlinks.append(link)
    return internal_outlinks, external_outlinks
