news-duplicated
===============

文本去重算法，研究自推荐系统中新闻的去重，采用了雅虎的Near-duplicates and shingling算法，服务端用c实现，客户端用java实现，利用thrift框架进行通信，为了提高扩展性，去重可以在服务端实现，服务器也提供了计算的接口，方便客户端自己扩展
