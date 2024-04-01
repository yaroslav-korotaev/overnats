import { type Destructible } from './types';

export class Autodestructible {
  private _destructibles: Destructible[];
  
  constructor() {
    this._destructibles = [];
  }
  
  protected use<T extends Destructible>(destructible: T): T {
    this._destructibles.push(destructible);
    
    return destructible;
  }
  
  protected release(disposable: Destructible): void {
    this._destructibles.splice(this._destructibles.indexOf(disposable), 1);
  }
  
  public async destroy(): Promise<void> {
    for (let i = this._destructibles.length - 1; i >= 0; i--) {
      await this._destructibles[i].destroy();
    }
  }
}
