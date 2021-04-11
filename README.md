# SachinSoni_BCG_CaseStudy

Use below command to run the Case Study and Analyse Output on a cluster :

spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --class CaseStudyAnalysis \
   <path-of-jar-file>/SachinSoni_BCG_CaseStudy.jar src/main/resources/data
