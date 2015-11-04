---
title: Scalaris &mdash; Maven Repository
root: .
layout: default
---

# Maven Repository Setup

To use this repository, add it to your `pom.xml`:

{% highlight xml %}
<dependencies>
  <dependency>
    <groupId>de.zib.scalaris</groupId>
    <artifactId>java-api</artifactId>
      <version>[0.7.2,)</version>
  </dependency>
</dependencies>

<repositories>
  <repository>
    <id>scalaris-repo</id>
    <url>https://scalaris-team.github.io/scalaris/maven</url>
  </repository>
</repositories>
{% endhighlight %}
