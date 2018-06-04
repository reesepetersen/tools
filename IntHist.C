#include "TFile.h"
#include "TH1.h"
#include "TH2.h"
#include "TLegend.h"
#include <fstream>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include "TGraph.h"
#include <cmath>
#include <regex>

class integrator {
  public:
    void IntHist(const string file_in, const string sub_folder = "", const string direction = "forward", bool normalize = true, const string output_dir = "", int verbose = 0);
  private:
};
//This is a function designed to produce an integrated version of the 1D histogram you put in.

void integrator::IntHist(string file_in, string sub_folder = "", string direction = "forward", bool normalize = true, const string output_dir = "", int verbose = 0) {

  if (verbose == 1) {cout<<"Function called..."<<endl;}
//Create a TFile for the incoming file.
  TFile FileIn(file_in.c_str());

  if (verbose == 1) {cout<<"TFile created from infile..."<<endl;}
//Check if file is already open. Quit if it is.
  if (!FileIn.IsOpen()) {

    if (verbose == 1) {cout<<"File is already open, quitting..."<<endl;}

    return;
  }
  
//Get the requested histogram from the file.
  TH1* RawHist;
  if (sub_folder != "") {

    if (verbose == 1) {cout<<"sub_folder given: "<<sub_folder<<endl;}

    RawHist = (TH1*)(FileIn.Get(sub_folder.c_str())->Clone());

  }
  else {

    if (verbose == 1) {cout<<"no sub_folder given"<<endl;}

    RawHist = (TH1*)(FileIn.Get("")->Clone());

  }
  RawHist->SetDirectory(0);

//Perform the integration in the given direction and normalize, unless told otherwise.
  TH1* IntegratedHist = (TH1*)(RawHist->Clone());
  IntegratedHist->SetDirectory(0);
  double integral;
  if (normalize) {
    integral = RawHist->Integral();
  }
  else {
    integral = 1.0;
  }
  double running_sum = 0;
  if (direction == "forward") {
    for (int i=1; i<=RawHist->GetXaxis()->GetNbins(); i++) {
      running_sum += RawHist->GetBinContent(i);
      IntegratedHist->SetBinContent(i,running_sum/integral);
    }
  }
  else if (direction == "backward") {
    for (int i=RawHist->GetXaxis()->GetNbins(); i==1; i--) {
      running_sum += RawHist->GetBinContent(i);
      IntegratedHist->SetBinContent(i,running_sum/integral);
    }
  }
  else {
    cout<<"Options for 'direction' are 'forward' and 'backward', can't do "<<direction<<endl;
    return;
  }
//Add "analyzer_subfolder" or something like that and "_integrated" to the end of the file name
  regex regex_file("([^/]+)[.]root");
  smatch matches;
  regex_search(file_in,matches,regex_file);
  string file_name = matches[1];
  if (verbose == 1) {cout<<"filename found: "<<file_name<<endl;}
  //Find all the / in sub_folder:
  int slash;
  vector <int> slashes;
  for (int i = 0; i < sub_folder.length(); i++) {
    int slash=sub_folder.find("/",i);
    if (slash == std::string::npos) {
      i = sub_folder.length();
    }
    else {
      slashes.push_back(slash);
      i = slash;
      if (verbose == 1) {cout<<"slash at: "<<slash<<endl;}
    }
  }
  //Replace all / with _:
  string sub_folder_ = sub_folder;
  for (int i = 0; i < slashes.size(); i++) {
    sub_folder_.replace(slashes[i],1,"_");
    if (verbose == 1) {cout<<"sub_folder_: "<<sub_folder_<<endl;}
  }
  string new_file_name;
  //FIX THIS:
  if (output_dir == "") {
    string name_in = file_in;
    name_in.erase(name_in.end()-5,name_in.end());
    if (verbose == 1) {cout<<"name_in: "<<name_in<<endl;}
    new_file_name = name_in+"_"+sub_folder_+"_integrated.root";
    if (verbose == 1) {cout<<"new_file_name: "<<new_file_name<<endl;}
  }
  else {
    new_file_name = output_dir+file_name+"_"+sub_folder_+"_integrated.root";
  }
//Save the integrated histogram
  TFile IntFile(new_file_name.c_str(),"CREATE");
  IntegratedHist->Write();
  IntFile.Close();
}
