package se.fk.github.rimfrost.process.template;

import org.junit.jupiter.api.Test;
import se.fk.rimfrost.framework.process.helpers.ProcessbeskrivningVerifier;

//TODO Remove this test together with processbeskrivning.json if the process has no processvy
class TemplateProcessbeskrivningTest
{
   //TODO Change to the process's BPMN file
   @Test
   void processbeskrivning_should_contain_all_aktiviteter()
   {
      ProcessbeskrivningVerifier.assertAllAktiviteterInProcessbeskrivning("template_process.bpmn");
   }
}
