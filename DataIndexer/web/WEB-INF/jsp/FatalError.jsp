<%@ include file="/WEB-INF/jsp/Include.jsp" %>

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<title>Fatal Error </title>
<meta name="Author" content="John Fereira">
<meta name="Keywords" content="Fatal Error">
<meta name="Description" content="Fatal Error "> 

</head>
<body >  

<div class="content">
   <h3>Fatal Error </h3>

   <div>
   Something is very wrong with the application.  Please contact the DataIndexer Administrator.
   </div>
   <div> 
   
   Summary: <c:out value="${dataIndexerError.summary}" /><br />
   Message: <c:out value="${dataIndexerError.message}" /><br /> 
   </div> 
</div> <!- end content div> 
</body>
</html>
