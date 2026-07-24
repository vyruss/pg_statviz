"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import importlib.resources
import matplotlib.pyplot as plt
import matplotlib.font_manager as fnt
from PIL import Image


MAX_POINTS = 100


def setup():
    for f in ["NotoSans-Regular.ttf", "NotoSans-SemiBold.ttf"]:
        f = importlib.resources.files("pg_statviz.libs").joinpath(f)
        fnt.fontManager.addfont(f)
    plt.rcParams['font.family'] = 'Noto Sans'
    plt.rcParams['font.size'] = 12
    plt.rcParams['lines.marker'] = 'o'
    base_image_path = importlib.resources.files("pg_statviz.libs")\
        .joinpath("pg_statviz.png")
    im = Image.open(str(base_image_path))
    height = im.size[1]
    fig = plt.figure(figsize=(19.2, 10.8))
    fig.figimage(im, 0, fig.bbox.ymax - height, zorder=3)
    plt.grid(visible=True)
    plt.ticklabel_format(axis='y', style='plain')
    plt.gcf().autofmt_xdate()
    return plt, fig


def setupdouble():
    plt = setup()[0]
    fig, (splt1, splt2) = plt.subplots(2, figsize=(19.2, 10.8))
    base_image_path = importlib.resources.files("pg_statviz.libs")\
        .joinpath("pg_statviz.png")
    im = Image.open(str(base_image_path))
    height = im.size[1]
    fig.figimage(im, 0, fig.bbox.ymax - height, zorder=3)
    for s in [splt1, splt2]:
        s.grid(visible=True)
        s.ticklabel_format(axis='y', style='plain')
    return plt, fig, splt1, splt2
