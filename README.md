# A Study on the Evolution of the Dutch Web

This repository contains the code used to generate the results for the research
titled "A Study on the Evolution of the Dutch Web" by Daan Kooij. This project
served as the graduation assignment for his Master degree in Computer Science at
the University of Twente.

## Abstract
Search engines need to have an up-to-date view of the Web, but Web crawling
resources are limited, meaning that not all pages can be crawled continuously.
With this research, the evolution of the Dutch Web is studied, having the
ultimate goal of generating insights that can be used to optimise the Web
crawlers of search engines.

As part of this research, daily crawls are performed on a large number of pages
from the Dutch Web, using a custom-built Web crawler that circumvents cookie
walls. This results in a novel high-quality dataset of the Dutch Web, which is
used to carry out a large-scale study on the evolution of the Dutch Web. This
study includes an investigation of change types, the discovery of temporal
change patterns, and the composition of a predictive Machine Learning model that
can predict whether the text on pages will change.

The main conclusions are the following. First, we discovered that over
two-thirds of the pages on the Dutch Web undergo some kind of change in a given
24-hour period, but that only just over half of these changes are informative,
with an even lesser fraction reflecting content being purposefully updated by
human beings. Second, we identified that change frequencies differ significantly
between different page features, but that all of them follow a weekly repeating
pattern. Moreover, we found that the change rates of most pages are stable.
Finally, we trained two types of Machine Learning models to predict whether page
text will change in the coming 24 hours. The highest performing one is a Random
Forest model, resulting in a _minimum recall_ of 78.48%. By explaining the
model's predictions and analysing prediction errors, we gained insights into the
relationships between page characteristics, change likelihood and prediction 
accuracy.

The insights gained by performing this study can be used by search providers to
optimise the Web crawlers of their search engines, for example by focusing
crawling resources on pages that tend to change often. Also the trained
predictive model can itself be used to guide the crawling processes of Web
crawlers, by prioritising crawling the pages for which the model predicts that
the text will change.
