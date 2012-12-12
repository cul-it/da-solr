<%@ include file="/WEB-INF/jsp/Include.jsp" %>

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<title>Show Triples Location </title>
<meta name="Author" content="John Fereira">
<meta name="Keywords" content="Show Triples Location">
<meta name="Description" content="Show Triples File Location"> 

</head>
<body >  

<div class="content">
   <h3>Show Triples Location for bibid: <c:out value="${bibid}" /></h3>
   <c:if test="${!empty fileUriList}" > 
   <p> 
   <c:forEach var="fileUri" items="${fileUriList}" >
   <c:out value="${fileUri}" /><br />
   </c:forEach>
   </p>
   </c:if>
   <c:if test="${empty fileUriList}" > 
   <p><strong>No Results Found.</strong>
   </c:if>
</div> <!- end content div> 
</body>
</html>
