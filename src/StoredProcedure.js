function write2 (partitionkey, events, expectedVersion, pendingEvents, projections) {

    if (events === undefined || events==null) events = [];
    if (expectedVersion === undefined) expectedVersion = -2;
    if (pendingEvents === undefined) pendingEvents = null;
    if (projections === undefined || projections==null) projections = {};
    
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();
  
    tryQueryAndUpdate();
  
    // Recursively queries for a document by id w/ support for continuation tokens.
    // Calls tryUpdate(document) as soon as the query returns a document.
    function tryQueryAndUpdate(continuation) {
      var query = {query: "select * from root r where r.id = @id and r.k = @k", parameters: [{name: "@id", value: "-1"},{name: "@p", value: partitionkey}]};
      var requestOptions = {continuation: continuation};
  
      var isAccepted = collection.queryDocuments(collectionLink, query, requestOptions, function (err, documents, responseOptions) {
        if (err) throw new Error("Error" + err.message);
  
        if (documents.length > 0) {
          // If the document is found, update it.
          // There is no need to check for a continuation token since we are querying for a single document.
          tryUpdate(documents[0], false);
        } else if (responseOptions.continuation) {
          // Else if the query came back empty, but with a continuation token; repeat the query w/ the token.
          // It is highly unlikely for this to happen when performing a query by id; but is included to serve as an example for larger queries.
          tryQueryAndUpdate(responseOptions.continuation);
        } else {
          // Else the snapshot does not exist; create snapshot
          var doc = {p:partitionkey, id:"-1", latest:-1, projections:{"_lowWatermark":{"base":-1}}};
          tryUpdate(doc, true);
        }
      });
  
      // If we hit execution bounds - throw an exception.
      // This is highly unlikely given that this is a query by id; but is included to serve as an example for larger queries.
      if (!isAccepted) {
        throw new Error("The stored procedure timed out.");
      }
    }
  
    function insertEvents()
    {
      for (i=0; i<events.length; i++) {
        var isAccepted = collection.createDocument(collectionLink, events[i], function (err, documentCreated) {
            if (err) throw new Error ("Create doc " + JSON.stringify(events[i]) + " failed");
        });

        // If we hit execution bounds - throw an exception.
        if (!isAccepted) {
            throw new Error("Unable to create document.");
        }
      }
    }

    function appendEvents(doc, events)
    {
        if (events.length>0) {
            if (doc.pendingEvents==null)
                doc.pendingEvents = {"base": parseInt(events[0].id)-1, "value": events};
            else
                Array.prototype.push.apply(doc.pendingEvents.value, events);
        }
    }
  
    // The kernel function
    function tryUpdate(doc, isCreate) {
  
      // DocumentDB supports optimistic concurrency control via HTTP ETag.
      var requestOptions = {etag: doc._etag};
  
      // Step 1: Insert events to DB
      if (expectedVersion==-2) {
          // thor mode
          var i;
          for (i=0; i<events.length; i++) {
              events[i].id = doc.latest+i+1;
          }
      } else if (doc.latest != expectedVersion) {
          throw new Error("Inconsistent expectedVersion; expected: " + expectedVersion + ", actually: " + doc.latest);
      }
      insertEvents();

      // Step 2: Update snapshot document's latest
      doc.latest = doc.latest + events.length
  
      // Step 3: Opt in/out for projections: null for opt out, else opt in, "_lowWatermark" cannot be opted out
      for (var key in projections) {
        if (projections.hasOwnProperty(key)) {
            if (projections[key]!=null) {
                doc.projections[key]=projections[key];
            }
            else {
                if (key!="_lowWatermark")
                    delete doc.projections[key];
            }
        }
      }

      // Step 4: Update pendingEvents: clear (input pendingEvents==null) / append old (from non-empty input pendingEvents) / append new (from input events)
      if (pendingEvents==null) {
        if (doc.pendingEvents!=null)
            delete doc["pendingEvents"];
      }
      else {
        appendEvents(doc, pendingEvents);  
        appendEvents(doc, events);
      }

      // Step 5: Trim pendingEvents based on min base of all projections
      if (doc.pendingEvents!=null )
      {
        var newBase = Object.values(doc.projections).reduce(function(prev, curr) {
            return prev.base < curr.base ? prev : curr;
        }).base;

        if (doc.pendingEvents.base < newBase) {
            doc.pendingEvents.value = doc.pendingEvents.value.filter(value => parseInt(value.id)>newBase);
            doc.pendingEvents.base = newBase;
            if (doc.pendingEvents.value.length==0)
                delete doc["pendingEvents"];
        }    
      }
  
      // Step 6: Replace existing snapshot document or create the first snapshot document for this partition key
      if (!isCreate)
      {
          var isAccepted = collection.replaceDocument(doc._self, doc, requestOptions, function (err, updatedDocument, responseOptions) {
            if (err) throw new Error("Error" + err.message);
            console.log("etag of replaced document is: %s", updatedDocument._etag);
            getContext().getResponse().setBody(updatedDocument._etag);
          });
  
          // If we hit execution bounds - throw an exception.
          if (!isAccepted) {
            throw new Error("Unable to replace snapshot document.");
          }
      }
      else {
          var isAccepted = collection.createDocument(collectionLink, doc, function (err, documentCreated) {
              if (err) throw new Error ("Create doc " + JSON.stringify(doc) + " failed");
              console.log("etag of created document is: %s", documentCreated._etag);
              getContext().getResponse().setBody(documentCreated._etag);
          });
          
          // If we hit execution bounds - throw an exception.
          if (!isAccepted) {
            throw new Error("Unable to create snapshot document.");
          }
      }
    }
  }
