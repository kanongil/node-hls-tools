var util = require('util');

module.exports = oncemore;

// apply oncemore() to an emitter, and enable it to accept multiple events as input 
function oncemore(emitter) {
  if (!emitter) return emitter;

  var once = emitter.once;
  if (once && !once._old) {
    emitter.once = function(type, listener) {
      if (arguments.length <= 2)
        return once.apply(this, arguments);

      var types = Array.prototype.slice.call(arguments, 0, -1);
      var listener = arguments.length ? arguments[arguments.length-1] : undefined;
      if (typeof listener !== 'function')
        throw TypeError('listener must be a function');

      function g() {
        types.forEach(function(type) {
          this.removeListener(type, g);
        }, this);

        listener.apply(this, arguments);
      }
      g.listener = listener;

      types.forEach(function(type) {
        this.on(type, g);
      }, this);

      return this;
    };
    emitter.once._old = once;
  }

  return emitter;
}
