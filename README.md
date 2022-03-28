# A Study on the Evolution of the Dutch Web

This repository contains the code used to generate the results for the research
titled "A Study on the Evolution of the Dutch Web" by Daan Kooij. This project
served as the graduation assignment for his Master degree in Computer Science at
the University of Twente.

## Abstract
This study investigates the evolution of the Dutch Web. In particular, it
examines how Web page content and links between pages change over time. This is
done by performing daily Web crawls on over 150.000 Web pages in the _.nl_
domain, whose URLs are extracted from the Dutch Wikipedia. The main novelties
of the work are that it is the first study to investigate the dynamics of the
general Dutch Web, and that the custom-made crawler bypasses "cookie walls" in
an age post-GDPR.

This work has two main objectives. First, it provides insights about the change
behaviour of pages on the Dutch Web, by investigating different types of
changes and by studying change timelines. Second, it builds a predictive model
that predicts text changes to pages on the Dutch Web.

The main conclusions are the following. First, we discovered that over
two-thirds of the pages on the Dutch Web undergo some kind of change in a given
24-hour period, but that only just over half of them are informative, with an
even lesser fraction reflecting content being purposefully updated by human
beings. Second, we identified that change frequencies differ significantly
between different page features, but that all of them follow a weekly repeating
pattern. Moreover, we found that the change rates of most pages are stable.
Finally, we trained two types of Machine Learning models to predict whether
page text will change in the coming 24 hours. The highest performing one is a
Random Forest model, resulting in a _minimum recall_ of 78.48%.
