EchoQuery
=======

*Gabe Lyons, Vinh Tran, Prof. Carsten Binnig, Prof. Ugur Cetintemel, Prof. Tim Kraska @ Brown University*

A interactive spoken dialogue interface for querying relational databases built on top
of Amazon Echo/Alexa.

Demo
----

[![Demo on Vimeo](https://raw.github.com/vqtran/EchoQuery/master/public/resources/vimeo.png)](http://www.vimeo.com/vqtran/EchoQuery)

Abstract
--------

Recent advances in automatic speech recognition and natural language processing have led to a new generation of robust voice-based interfaces. Yet, there is very little work on using voice-based interfaces to query database systems. In fact, one might even wonder who in her right mind would want to query a database system using voice commands!
With this project, we make the case for querying database systems using a voice-based interface, a new querying and interaction paradigm we call Query-by-Voice (QbV ). The aim of this project is to demonstrate the practicality and utility of QbV for relational DBMSs using a using a proof-of-concept system called EchoQuery. To achieve a smooth and intuitive interaction, the query interface of EchoQuery is inspired by casual human-to-human conversations.

The main features of the voice-based interface of EchoQuery are:

   __Hands-free Access:__ EchoQuery does not require the user to press a button or start an application using a gesture or a mouse-click. Instead, users can interact with the database by solely using their voice at any time.

   __Dialogue-based Querying:__ While traditional database systems provide a one-shot (i.e., stateless) query interface, natural language conversations are incremental (i.e., stateful) in nature. To that end, EchoQuery provides a stateful dialogue-based query interface between the user and the database where (1) users can start a conversation with an initial query and refine that query incrementally over time, and (2) EchoQuery can seek for clarification if the query is incomplete or has some ambiguities that need to be resolved.

   __Personalizable Vocabulary:__ Domain experts often use their own terms to formulate queries, which might be different from the schema elements (i.e., table and column names) of a database. Learning the terminology of a user and its translation to the underlying schema is similar to the problem of constructing a schema mapping in data integration.EchoQuery constructs these mappings incrementally on a per-user basis by issuing clarification questions using its dialogue-based query interface.

For more information see our [SIGMOD 2016 demonstration proposal](https://github.com/vqtran/EchoQuery/blob/master/public/resources/EchoQuery%20-%20SIGMOD%202016%20Demo%20Proposal.pdf).


