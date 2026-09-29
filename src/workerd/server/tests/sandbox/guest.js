export default {
  async fetch(request) {
    return new Response(
      `guest:${new URL(request.url).pathname}:${await request.text()}`
    );
  },
};
