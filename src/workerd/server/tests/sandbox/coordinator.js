export default {
  fetch(request, env) {
    return env.GUEST.fetch(request);
  },
};
