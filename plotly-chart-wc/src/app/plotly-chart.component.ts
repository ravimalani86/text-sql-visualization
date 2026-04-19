import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  SimpleChanges,
  ViewChild,
} from '@angular/core';
import { Chart, registerables, type ChartConfiguration } from 'chart.js';

Chart.register(...registerables);

export type ChartJsJsonConfig = ChartConfiguration;

@Component({
  selector: 'plotly-chart',
  standalone: true,
  template: `<canvas #canvas class="chart-canvas"></canvas>`,
  styles: [
    `
      :host {
        display: block;
      }
      .chart-canvas {
        width: 100%;
        height: 100%;
        display: block;
      }
    `,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PlotlyChartComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() config: ChartJsJsonConfig | null = null;

  @ViewChild('canvas', { static: true })
  private canvas!: ElementRef<HTMLCanvasElement>;

  private viewReady = false;
  private chart: Chart | null = null;

  ngAfterViewInit(): void {
    this.viewReady = true;
    void this.render();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if ('config' in changes) {
      void this.render();
    }
  }

  ngOnDestroy(): void {
    this.destroyChart();
  }

  private async render(): Promise<void> {
    if (!this.viewReady) return;

    const cfg = this.config;

    if (!cfg || typeof cfg !== 'object') {
      this.destroyChart();
      return;
    }

    const normalized: ChartConfiguration = {
      ...(cfg as ChartConfiguration),
      options: {
        ...(cfg.options ?? {}),
        responsive: true,
        maintainAspectRatio: false,
      },
    };

    this.destroyChart();
    this.chart = new Chart(this.canvas.nativeElement, normalized);
  }

  private destroyChart(): void {
    try {
      this.chart?.destroy();
    } catch {
      // best-effort cleanup only
    } finally {
      this.chart = null;
    }
  }
}
