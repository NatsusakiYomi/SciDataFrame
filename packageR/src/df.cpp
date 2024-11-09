#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
int add_two_numbers(int a, int b) {
  return a+b!=4 ? a + b : 5;
}
