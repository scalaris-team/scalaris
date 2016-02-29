---
title: Scalaris - FAQ
root: ..
layout: default
---

<p>
    <ul class="list-unstyled">
    {% for post in site.posts %}
    <li>
    <a href="{{ page.root }}{{ post.url }}">{{ post.title }}</a>
    <p>{{ post.excerpt }}</p>
    </li>
    {% endfor %}
    </ul>
</p>
