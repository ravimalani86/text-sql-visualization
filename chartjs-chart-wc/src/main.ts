import 'zone.js';
import { createCustomElement } from '@angular/elements';
import { createApplication } from '@angular/platform-browser';
import { ChartjsChartComponent } from './app/chartjs-chart.component';

async function defineChartjsChartElement(): Promise<void> {
  const app = await createApplication({ providers: [] });
  const element = createCustomElement(ChartjsChartComponent, { injector: app.injector });

  if (!customElements.get('chartjs-chart')) {
    customElements.define('chartjs-chart', element);
  }
}

defineChartjsChartElement().catch((err) => console.error(err));
