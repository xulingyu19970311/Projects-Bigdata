object Lab {
def stringSum(a: Int, b: Int=0)= {(a+b).toString}
def even(n: Int):Vector[Int] ={Vector.tabulate(n){x => x*2}}
def threshold(w: Double, x: Double, b: Double):Int ={
if(w*x>b)1 else 0}
def twice(fn: Double=> Double)(z: Double): Double = {fn(fn(z))}
}
