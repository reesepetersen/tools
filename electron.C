// Get the momentum of an electron from its energy

#include <cstdio>
#include <cmath>
#include <string>
#include <sstream>

class electron {
  public:
    void GetMomentum();
    void SetEnergy(double energy_in);
  private:
    double energy = 0.0;
    double momentum = 0.0;
    double mass = 0.51109989;
};

void electron::SetEnergy(double energy_in) {
  energy = energy_in;
}

void electron::GetMomentum() {
  double gamma = energy/mass;
  double beta = sqrt(1-1/pow(gamma,2));
  printf("p (MeV/c) = %.9f\n",energy*beta);
}
