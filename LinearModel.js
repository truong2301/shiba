'use strict';

function LinearModel() {
  this.n     = 0;
  this.sxy   = 0;
  this.sx    = 0;
  this.sy    = 0;
  this.alpha = 0;
  this.beta  = 0;
}

LinearModel.prototype.add = function(x,y) {

  this.sxy += x * y;
  this.sx  += x;
  this.sy  += y;
  this.sx2 += x * x;

  this.beta  =
    (this.sxy - this.sx * this.sy / this.n) /
    // TODO: Check for divide-by-zero
    (this.sx2 - this.sx * this.sx / this.n);
  this.alpha =
    (this.sy - this.beta * this.sx) / this.n;
};

LinearModel.prototype.evaluate = function(x) {
  return this.alpha + x * this.beta;
};

module.exports = LinearModel;
