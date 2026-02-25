import HeroSection from "@/components/Home/HeroSection";
import PMIExplainer from "@/components/Home/PMIExplainer";
import SpotlightComparison from "@/components/Home/SpotlightComparison";
import Visualizations from "@/components/Home/Visualizations";
import StatLeaderboards from "@/components/Home/StatLeaderboards";
import HowItWorks from "@/components/Home/HowItWorks";

const Index = () => {
  return (
    <main>
      <HeroSection />
      <PMIExplainer />
      <SpotlightComparison />
      <Visualizations />
      <StatLeaderboards />
      <HowItWorks />
    </main>
  );
};

export default Index;
