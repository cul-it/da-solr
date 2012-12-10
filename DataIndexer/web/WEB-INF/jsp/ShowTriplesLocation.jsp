<%@ include file="/WEB-INF/jsp/Include.jsp" %>

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<title>Show Triples Location </title>
<meta name="Author" content="John Fereira">
<meta name="Keywords" content="Fatal Error">
<meta name="Description" content="Fatal Error "> 

</head>
<body >  

<div class="content">
   <h3>Show Triples Location for bibid: <c:out value="${bibid}" /></h3>
   
   <p> 
   <c:forEach var="fileUri" items="${fileUriList}" >
   <c:out value="${fileUri}" /><br />
   </c:forEach>
   </p> 
</div> <!- end content div> 
</body>
</html>
