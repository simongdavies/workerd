// Sample handler: echo the marshalled request back as JSON.
//
// Shows what the host marshals into the `event`: the method, URL, request headers
// (as [name, value] pairs, preserving duplicates), and the whole request body. A
// UTF-8 body arrives as a string (bodyEncoding "utf-8"); a binary body is base64
// (bodyEncoding "base64", decode with atob); an empty body is null ("none").
//
// A plain (non-Response) return value is served as 200 application/json.
//
//   curl -X POST -H "X-Custom: hi" --data "the body" "localhost:8080/echo?q=1"
async function handler(event) {
  const headers = new Headers(event.headers);
  return {
    method: event.method,
    url: event.url,
    bodyEncoding: event.bodyEncoding,
    body: event.body,
    headerCount: event.headers.length,
    contentType: headers.get("content-type"),
    custom: headers.get("x-custom"),
  };
}

export { handler };
